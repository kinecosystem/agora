package transaction

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/agora-common/headers"
	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/memo"
	"github.com/kinecosystem/agora-common/solana/system"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/kinecosystem/agora-common/webhook/signtransaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	xrate "golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"

	"github.com/kinecosystem/agora/pkg/app"
	appconfigdb "github.com/kinecosystem/agora/pkg/app/memory"
	appmapper "github.com/kinecosystem/agora/pkg/app/memory/mapper"
	"github.com/kinecosystem/agora/pkg/rate"
	"github.com/kinecosystem/agora/pkg/testutil"
	"github.com/kinecosystem/agora/pkg/webhook"
)

type testEnv struct {
	auth   Authorizer
	config app.ConfigStore
	mapper app.Mapper

	ctx           context.Context
	subsidizer    ed25519.PublicKey
	subsidizerKey ed25519.PrivateKey
	mint          ed25519.PublicKey
}

func setup(t *testing.T) (env testEnv) {
	var err error

	env.ctx, err = headers.ContextWithHeaders(context.Background())
	require.NoError(t, err)
	env.config = appconfigdb.New()
	env.mapper = appmapper.New()
	env.mint = testutil.GenerateSolanaKeys(t, 1)[0]
	env.subsidizerKey = testutil.GenerateSolanaKeypair(t)
	env.subsidizer = env.subsidizerKey.Public().(ed25519.PublicKey)
	env.auth, err = NewAuthorizer(
		env.mapper,
		env.config,
		webhook.NewClient(http.DefaultClient),
		NewLimiter(func(r int) rate.Limiter { return rate.NewLocalRateLimiter(xrate.Limit(r)) }, 10, 5),
		env.subsidizerKey,
		env.mint,
		10,
	)
	require.NoError(t, err)

	return env
}

func TestAuthorizer_BadParse(t *testing.T) {
	env := setup(t)

	keys := testutil.GenerateSolanaKeys(t, 2)

	tx := solana.NewTransaction(
		env.subsidizer,
		system.CreateAccount(
			env.subsidizer,
			keys[0],
			keys[1],
			10,
			10,
		),
	)

	_, err := env.auth.Authorize(env.ctx, tx, nil, false)
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestAuthorizer_KeyProtections(t *testing.T) {
	env := setup(t)

	keys := testutil.GenerateSolanaKeys(t, 2)

	create, _, err := token.CreateAssociatedTokenAccount(
		env.subsidizer,
		keys[0],
		keys[1],
	)
	require.NoError(t, err)

	inputs := []solana.Transaction{
		// Creation (protect against invalid mints)
		solana.NewTransaction(
			env.subsidizer,
			create,
		),
		// Transfer
		solana.NewTransaction(
			env.subsidizer,
			token.Transfer(
				keys[0],
				keys[1],
				env.subsidizer,
				100,
			),
		),
	}

	for i := range inputs {
		_, err := env.auth.Authorize(env.ctx, inputs[i], nil, false)
		assert.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	}
}

func TestAuthorizer_AppValidation(t *testing.T) {
	env := setup(t)

	keys := testutil.GenerateSolanaKeys(t, 3)

	//
	// AppID - Not Found (expect signed by subsidizer)
	//
	tx := solana.NewTransaction(
		env.subsidizer,
		memo.Instruction("1-test"),
		token.Transfer(
			keys[0],
			keys[1],
			keys[2],
			10,
		),
	)
	auth, err := env.auth.Authorize(
		env.ctx,
		tx,
		nil,
		false,
	)
	assert.NoError(t, err)
	signed := tx
	require.NoError(t, signed.Sign(env.subsidizerKey))
	assert.Equal(t, AuthorizationResultOK, auth.Result)
	assert.EqualValues(t, signed.Signatures[0][:], auth.SignResponse.Signature)

	//
	// AppIndex - Not Found (expected signed by subsidizer)
	//
	tx = solana.NewTransaction(
		env.subsidizer,
		getInvoiceMemoInstruction(t, kin.TransactionTypeSpend, 10, 1),
		token.Transfer(
			keys[0],
			keys[1],
			keys[2],
			20,
		),
	)
	auth, err = env.auth.Authorize(
		env.ctx,
		tx,
		nil,
		false,
	)
	assert.NoError(t, err)
	signed = tx
	require.NoError(t, signed.Sign(env.subsidizerKey))
	assert.Equal(t, AuthorizationResultOK, auth.Result)
	assert.EqualValues(t, signed.Signatures[0][:], auth.SignResponse.Signature)

	//
	// AppID/AppIndex mismatch (rejected)
	//
	assert.NoError(t, env.mapper.Add(env.ctx, "test", 10))
	tx = solana.NewTransaction(
		env.subsidizer,
		memo.Instruction("1-test"),
		token.Transfer(
			keys[0],
			keys[1],
			keys[2],
			10,
		),
		getInvoiceMemoInstruction(t, kin.TransactionTypeSpend, 20, 1),
		token.Transfer(
			keys[0],
			keys[1],
			keys[2],
			10,
		),
	)
	auth, err = env.auth.Authorize(
		env.ctx,
		tx,
		nil,
		false,
	)
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.True(t, strings.Contains(err.Error(), "does not match registered"))

	//
	// AppID/AppIndex not found (rejected, cannot verify the mapping)
	//
	tx = solana.NewTransaction(
		env.subsidizer,
		memo.Instruction("1-beta"),
		token.Transfer(
			keys[0],
			keys[1],
			keys[2],
			10,
		),
		getInvoiceMemoInstruction(t, kin.TransactionTypeSpend, 20, 1),
		token.Transfer(
			keys[0],
			keys[1],
			keys[2],
			10,
		),
	)
	auth, err = env.auth.Authorize(
		env.ctx,
		tx,
		nil,
		false,
	)
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.True(t, strings.Contains(err.Error(), "is not registered"), err.Error())
}

func TestAuthorizer_CreateAccountValues(t *testing.T) {
	env := setup(t)
	keys := testutil.GenerateSolanaKeys(t, 3)
	tx := solana.NewTransaction(
		env.subsidizer,
		system.CreateAccount(
			env.subsidizer,
			keys[0],
			token.ProgramKey,
			env.auth.(*authorizer).minLamports+1,
			token.AccountSize,
		),
		token.InitializeAccount(
			keys[0],
			env.mint,
			keys[1],
		),
		token.SetAuthority(
			keys[0],
			keys[1],
			env.subsidizer,
			token.AuthorityTypeCloseAccount,
		),
	)

	_, err := env.auth.Authorize(env.ctx, tx, nil, false)
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.True(t, strings.Contains(err.Error(), "incorrect amount of min lamports"))

	tx = solana.NewTransaction(
		env.subsidizer,
		system.CreateAccount(
			env.subsidizer,
			keys[0],
			token.ProgramKey,
			env.auth.(*authorizer).minLamports,
			token.AccountSize+1,
		),
		token.InitializeAccount(
			keys[0],
			env.mint,
			keys[1],
		),
		token.SetAuthority(
			keys[0],
			keys[1],
			env.subsidizer,
			token.AuthorityTypeCloseAccount,
		),
	)

	_, err = env.auth.Authorize(env.ctx, tx, nil, false)
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.True(t, strings.Contains(err.Error(), "invalid size"))
}

func TestAuthorizer_CreateWithoutTransfer(t *testing.T) {
	env := setup(t)
	keys := testutil.GenerateSolanaKeys(t, 3)
	tx := solana.NewTransaction(
		env.subsidizer,
		system.CreateAccount(
			env.subsidizer,
			keys[0],
			token.ProgramKey,
			env.auth.(*authorizer).minLamports,
			token.AccountSize,
		),
		token.InitializeAccount(
			keys[0],
			env.mint,
			keys[1],
		),
		token.SetAuthority(
			keys[0],
			keys[1],
			env.subsidizer,
			token.AuthorityTypeCloseAccount,
		),
	)

	_, err := env.auth.Authorize(env.ctx, tx, nil, false)
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.True(t, strings.Contains(err.Error(), "created accounts must be a recipient of a transfer"))
}

func TestAuthorizer_RateLimiter(t *testing.T) {
	env := setup(t)

	subsidizer := testutil.GenerateSolanaKeypair(t)
	app4Tx, app4IL, app4Sig := generateTxData(t, subsidizer, 4)

	//
	// Apps 1, 2, and 3 are subsidized, while App 4 is not (no limit)
	//
	err := env.config.Add(context.Background(), 1, &app.Config{
		AppName:       "some name",
		WebhookSecret: generateWebhookSecret(t),
	})
	require.NoError(t, err)
	err = env.config.Add(context.Background(), 2, &app.Config{
		AppName:       "some other name",
		WebhookSecret: generateWebhookSecret(t),
	})
	require.NoError(t, err)
	err = env.config.Add(context.Background(), 3, &app.Config{
		AppName:       "yet another name",
		WebhookSecret: generateWebhookSecret(t),
	})
	require.NoError(t, err)

	var callCount int64
	webhookResp := &signtransaction.SuccessResponse{
		Signature: app4Sig,
	}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)
	testServer := newTestServerWithJSONResponse(t, http.StatusOK, b, &callCount)

	// Setup webhook mapping
	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	err = env.config.Add(context.Background(), 4, &app.Config{
		AppName:            "indeed another name",
		WebhookSecret:      generateWebhookSecret(t),
		SignTransactionURL: signURL,
	})
	require.NoError(t, err)

	// Prime the limiter for 1
	for i := 0; i < 5; i++ {
		tx, il, _ := generateTxData(t, env.subsidizerKey, 1)
		result, err := env.auth.Authorize(env.ctx, tx, il, false)
		assert.NoError(t, err)
		assert.Equal(t, AuthorizationResultOK, result.Result)
	}

	// Hit the limit for 1
	//
	// note: we try a few times in case the system is slow.
	var rateLimited bool
	for i := 0; i < 2; i++ {
		tx, il, _ := generateTxData(t, env.subsidizerKey, 1)
		_, err = env.auth.Authorize(env.ctx, tx, il, false)
		assert.Error(t, err)
		if status.Code(err) == codes.ResourceExhausted {
			rateLimited = true
			break
		}
	}
	assert.True(t, rateLimited)

	// 2 should be fine. Prime it's limiter to also hit global
	for i := 0; i < 5; i++ {
		tx, il, _ := generateTxData(t, env.subsidizerKey, 2)
		result, err := env.auth.Authorize(env.ctx, tx, il, false)
		assert.NoError(t, err)
		assert.Equal(t, AuthorizationResultOK, result.Result)
	}

	// Global limit should impact 3
	for i := 0; i < 5; i++ {
		tx, il, _ := generateTxData(t, env.subsidizerKey, 3)
		_, err = env.auth.Authorize(env.ctx, tx, il, false)
		assert.Error(t, err)
		if status.Code(err) == codes.ResourceExhausted {
			rateLimited = true
			break
		}
	}
	assert.True(t, rateLimited)

	// But not 4, who has their own subsidization
	for i := 0; i < 10; i++ {
		result, err := env.auth.Authorize(env.ctx, app4Tx, app4IL, false)
		assert.NoError(t, err)
		assert.Equal(t, AuthorizationResultOK, result.Result)
		assert.Equal(t, app4Sig, result.SignResponse.Signature)
	}
}

func TestAuthorizer_Webhook_Subsidized(t *testing.T) {
	env := setup(t)

	// Setup webhook with a successful (empty) response
	var callCount int64
	webhookResp := &signtransaction.SuccessResponse{}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)
	testServer := newTestServerWithJSONResponse(t, http.StatusOK, b, &callCount)

	// Setup webhook mapping
	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	require.NoError(t, env.config.Add(env.ctx, 1, &app.Config{
		AppName:            "myapp",
		SignTransactionURL: signURL,
		WebhookSecret:      generateWebhookSecret(t),
	}))

	tx, il, sig := generateTxData(t, env.subsidizerKey, 1)
	result, err := env.auth.Authorize(env.ctx, tx, il, false)
	require.NoError(t, err)
	assert.Equal(t, AuthorizationResultOK, result.Result)
	assert.Equal(t, sig, result.SignResponse.Signature)
	assert.EqualValues(t, 1, atomic.LoadInt64(&callCount))
}

func TestAuthorizer_Webhook_Signature(t *testing.T) {
	env := setup(t)

	signer := testutil.GenerateSolanaKeypair(t)
	tx, il, sig := generateTxData(t, signer, 1)

	// Setup webhook with a successful (empty) response
	var callCount int64
	webhookResp := &signtransaction.SuccessResponse{
		Signature: sig,
	}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)
	testServer := newTestServerWithJSONResponse(t, http.StatusOK, b, &callCount)

	// Setup webhook mapping
	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	require.NoError(t, env.config.Add(env.ctx, 1, &app.Config{
		AppName:            "myapp",
		SignTransactionURL: signURL,
		WebhookSecret:      generateWebhookSecret(t),
	}))

	result, err := env.auth.Authorize(env.ctx, tx, il, false)
	require.NoError(t, err)
	assert.Equal(t, AuthorizationResultOK, result.Result)
	assert.Equal(t, sig, result.SignResponse.Signature)
	assert.EqualValues(t, 1, atomic.LoadInt64(&callCount))
}

func TestAuthorizer_Webhook_NoSignature(t *testing.T) {
	env := setup(t)

	signer := testutil.GenerateSolanaKeypair(t)
	tx, il, _ := generateTxData(t, signer, 1)

	// Setup webhook with a successful (empty) response
	var callCount int64
	webhookResp := &signtransaction.SuccessResponse{}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)
	testServer := newTestServerWithJSONResponse(t, http.StatusOK, b, &callCount)

	// Setup webhook mapping
	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	require.NoError(t, env.config.Add(env.ctx, 1, &app.Config{
		AppName:            "myapp",
		SignTransactionURL: signURL,
		WebhookSecret:      generateWebhookSecret(t),
	}))

	result, err := env.auth.Authorize(env.ctx, tx, il, false)
	require.NoError(t, err)
	assert.Equal(t, AuthorizationResultPayerRequired, result.Result)
	assert.Nil(t, result.SignResponse)
	assert.EqualValues(t, 1, atomic.LoadInt64(&callCount))
}

func TestAuthorizer_Webhook_400(t *testing.T) {
	env := setup(t)

	signer := testutil.GenerateSolanaKeypair(t)
	tx, il, _ := generateTxData(t, signer, 1)

	// Setup webhook with a successful (empty) response
	var callCount int64
	testServer := newTestServerWithJSONResponse(t, http.StatusBadRequest, []byte{}, &callCount)

	// Setup webhook mapping
	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	require.NoError(t, env.config.Add(env.ctx, 1, &app.Config{
		AppName:            "myapp",
		SignTransactionURL: signURL,
		WebhookSecret:      generateWebhookSecret(t),
	}))

	_, err = env.auth.Authorize(env.ctx, tx, il, false)
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.EqualValues(t, 1, atomic.LoadInt64(&callCount))
}

func TestAuthorizer_Webhook_403(t *testing.T) {
	env := setup(t)

	signer := testutil.GenerateSolanaKeypair(t)
	tx, il, _ := generateTxData(t, signer, 1)

	// Setup webhook with a successful (empty) response
	var callCount int64
	webhookResp := &signtransaction.ForbiddenResponse{
		Message: "some message",
	}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)
	testServer := newTestServerWithJSONResponse(t, http.StatusForbidden, b, &callCount)

	// Setup webhook mapping
	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	require.NoError(t, env.config.Add(env.ctx, 1, &app.Config{
		AppName:            "myapp",
		SignTransactionURL: signURL,
		WebhookSecret:      generateWebhookSecret(t),
	}))

	resp, err := env.auth.Authorize(env.ctx, tx, il, false)
	require.NoError(t, err)
	assert.Equal(t, AuthorizationResultRejected, resp.Result)
	assert.Empty(t, resp.InvoiceErrors)
	assert.EqualValues(t, 1, atomic.LoadInt64(&callCount))
}

func TestAuthorizer_Webhook_ErrorMappings(t *testing.T) {
	env := setup(t)

	signer := testutil.GenerateSolanaKeypair(t)
	tx, il, _ := generateTxData(t, signer, 1)

	// Setup webhook with a successful (empty) response
	var callCount int64
	webhookResp := &signtransaction.ForbiddenResponse{
		Message: "some message",
		InvoiceErrors: []signtransaction.InvoiceError{
			{
				OperationIndex: 0,
				Reason:         signtransaction.AlreadyPaid,
			},
			{
				OperationIndex: 1,
				Reason:         signtransaction.WrongDestination,
			},
			{
				OperationIndex: 2,
				Reason:         signtransaction.SKUNotFound,
			},
			{
				OperationIndex: 3,
				Reason:         "other",
			},
		},
	}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)
	testServer := newTestServerWithJSONResponse(t, http.StatusForbidden, b, &callCount)

	// Setup webhook mapping
	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	require.NoError(t, env.config.Add(env.ctx, 1, &app.Config{
		AppName:            "myapp",
		SignTransactionURL: signURL,
		WebhookSecret:      generateWebhookSecret(t),
	}))

	resp, err := env.auth.Authorize(env.ctx, tx, il, false)
	require.NoError(t, err)
	assert.Equal(t, AuthorizationResultInvoiceError, resp.Result)
	assert.Len(t, resp.InvoiceErrors, len(webhookResp.InvoiceErrors))
	assert.EqualValues(t, 1, atomic.LoadInt64(&callCount))

	assert.Equal(t, AuthorizationResultInvoiceError, resp.Result)
	assert.Equal(t, len(webhookResp.InvoiceErrors), len(resp.InvoiceErrors))

	assert.Equal(t, uint32(0), resp.InvoiceErrors[0].OpIndex)
	assert.Equal(t, commonpb.InvoiceError_ALREADY_PAID, resp.InvoiceErrors[0].Reason)
	assert.True(t, proto.Equal(il.Invoices[0], resp.InvoiceErrors[0].Invoice))

	assert.Equal(t, uint32(1), resp.InvoiceErrors[1].OpIndex)
	assert.Equal(t, commonpb.InvoiceError_WRONG_DESTINATION, resp.InvoiceErrors[1].Reason)
	assert.True(t, proto.Equal(il.Invoices[1], resp.InvoiceErrors[1].Invoice))

	assert.Equal(t, uint32(2), resp.InvoiceErrors[2].OpIndex)
	assert.Equal(t, commonpb.InvoiceError_SKU_NOT_FOUND, resp.InvoiceErrors[2].Reason)
	assert.True(t, proto.Equal(il.Invoices[2], resp.InvoiceErrors[2].Invoice))

	assert.Equal(t, uint32(3), resp.InvoiceErrors[3].OpIndex)
	assert.Equal(t, commonpb.InvoiceError_UNKNOWN, resp.InvoiceErrors[3].Reason)
	assert.True(t, proto.Equal(il.Invoices[3], resp.InvoiceErrors[3].Invoice))
}

func TestAuthorizer_Webhook_Propagation(t *testing.T) {
	env := setup(t)

	// Setup webhook with a successful (empty) response
	webhookResp := &signtransaction.SuccessResponse{}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)
	var signReq signtransaction.Request

	testServer := httptest.NewServer(http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		require.NoError(t, json.NewDecoder(req.Body).Decode(&signReq))
		_ = req.Body.Close()

		resp.WriteHeader(http.StatusOK)
		resp.Header().Set("Content-Type", "application/json")
		_, err := resp.Write(b)
		require.NoError(t, err)
	}))

	// Setup webhook mapping
	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	require.NoError(t, env.config.Add(env.ctx, 1, &app.Config{
		AppName:            "myapp",
		SignTransactionURL: signURL,
		WebhookSecret:      generateWebhookSecret(t),
	}))

	tx, il, sig := generateTxData(t, env.subsidizerKey, 1)
	result, err := env.auth.Authorize(env.ctx, tx, il, false)
	require.NoError(t, err)
	assert.Equal(t, AuthorizationResultOK, result.Result)
	assert.Equal(t, sig, result.SignResponse.Signature)

	assert.Equal(t, 4, signReq.KinVersion)
	assert.Equal(t, tx.Marshal(), signReq.SolanaTransaction)

	raw, err := proto.Marshal(il)
	require.NoError(t, err)
	assert.EqualValues(t, raw, signReq.InvoiceList)
}

func TestAuthorizer_IgnoreSigned(t *testing.T) {
	// If we construct a transaction, we expect the verifications to occur,
	// but no webhooks or rate limiting to occur.
	env := setup(t)

	keys := testutil.GenerateSolanaKeys(t, 2)

	tx := solana.NewTransaction(
		env.subsidizer,
		system.CreateAccount(
			env.subsidizer,
			keys[0],
			keys[1],
			10,
			10,
		),
	)
	require.NoError(t, tx.Sign(env.subsidizerKey))

	// Even though we've presigned it, we should be rejected due to bad form.
	_, err := env.auth.Authorize(env.ctx, tx, nil, true)
	assert.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	// Setup webhook with a successful (empty) response
	var callCount int64
	webhookResp := &signtransaction.SuccessResponse{}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)
	testServer := newTestServerWithJSONResponse(t, http.StatusOK, b, &callCount)

	// Setup webhook mapping
	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	require.NoError(t, env.config.Add(env.ctx, 1, &app.Config{
		AppName:            "myapp",
		SignTransactionURL: signURL,
		WebhookSecret:      generateWebhookSecret(t),
	}))

	tx, il, sig := generateTxData(t, env.subsidizerKey, 1)
	require.NoError(t, tx.Sign(env.subsidizerKey))

	// Ensure we get all the correct data, sans webhook call
	result, err := env.auth.Authorize(env.ctx, tx, il, true)
	require.NoError(t, err)
	assert.Equal(t, AuthorizationResultOK, result.Result)
	assert.Equal(t, sig, result.SignResponse.Signature)
	assert.EqualValues(t, 0, atomic.LoadInt64(&callCount))

	tx, il, sig = generateTxData(t, env.subsidizerKey, 2)
	require.NoError(t, tx.Sign(env.subsidizerKey))

	// Ensure no rate limits
	for i := 0; i < 10; i++ {
		result, err := env.auth.Authorize(env.ctx, tx, il, true)
		require.NoError(t, err)
		assert.Equal(t, AuthorizationResultOK, result.Result)
		assert.Equal(t, sig, result.SignResponse.Signature)
	}
}

func newTestServerWithJSONResponse(t *testing.T, statusCode int, b []byte, callCount *int64) *httptest.Server {
	testServer := httptest.NewServer(http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		resp.WriteHeader(statusCode)
		resp.Header().Set("Content-Type", "application/json")
		_, err := resp.Write(b)
		require.NoError(t, err)
		atomic.AddInt64(callCount, 1)
	}))
	return testServer
}

func generateWebhookSecret(t *testing.T) string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	require.NoError(t, err)
	return string(b)
}

func generateTxData(t *testing.T, signer ed25519.PrivateKey, appIndex uint16) (tx solana.Transaction, il *commonpb.InvoiceList, sig []byte) {
	var instructions []solana.Instruction

	if appIndex > 0 {
		il = &commonpb.InvoiceList{
			Invoices: []*commonpb.Invoice{
				{
					Items: []*commonpb.Invoice_LineItem{
						{
							Title: "LineItem1",
						},
					},
				},
				{
					Items: []*commonpb.Invoice_LineItem{
						{
							Title: "LineItem2",
						},
					},
				},
				{
					Items: []*commonpb.Invoice_LineItem{
						{
							Title: "LineItem3",
						},
					},
				},
				{
					Items: []*commonpb.Invoice_LineItem{
						{
							Title: "LineItem4",
						},
					},
				},
			},
		}

		raw, err := proto.Marshal(il)
		require.NoError(t, err)

		h := sha256.Sum224(raw)
		fk := make([]byte, 29)
		copy(fk, h[:])
		m, err := kin.NewMemo(1, kin.TransactionTypeSpend, appIndex, fk)
		require.NoError(t, err)

		instructions = []solana.Instruction{
			memo.Instruction(base64.StdEncoding.EncodeToString(m[:])),
		}
	}

	keys := testutil.GenerateSolanaKeys(t, 3)
	for i := 0; i < 4; i++ {
		instructions = append(instructions, token.Transfer(
			keys[0],
			keys[1],
			keys[2],
			10*(uint64(i)+1),
		))
	}

	tx = solana.NewTransaction(signer.Public().(ed25519.PublicKey), instructions...)
	require.NoError(t, tx.Sign(signer))
	sig = tx.Signature()
	tx.Signatures[0] = solana.Signature{}

	return tx, il, sig
}

func getInvoiceMemoInstruction(t *testing.T, txType kin.TransactionType, appIndex, transferCount int) solana.Instruction {
	il := &commonpb.InvoiceList{}
	for i := 0; i < transferCount; i++ {
		il.Invoices = append(il.Invoices, &commonpb.Invoice{
			Items: []*commonpb.Invoice_LineItem{
				{
					Title: "Item1",
				},
			},
		})
	}

	raw, err := proto.Marshal(il)
	require.NoError(t, err)

	h := sha256.Sum224(raw)
	fk := make([]byte, 29)
	copy(fk, h[:])

	m, err := kin.NewMemo(1, txType, uint16(appIndex), fk)
	require.NoError(t, err)

	return memo.Instruction(base64.StdEncoding.EncodeToString(m[:]))
}
