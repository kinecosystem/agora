package account

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/kinecosystem/agora-common/headers"
	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/memo"
	"github.com/kinecosystem/agora-common/solana/system"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/kinecosystem/agora-common/webhook/createaccount"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	xrate "golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
	env.subsidizerKey = testutil.GenerateSolanaKeypair(t)
	env.subsidizer = env.subsidizerKey.Public().(ed25519.PublicKey)
	env.mint = testutil.GenerateSolanaKeys(t, 1)[0]
	env.auth = NewAuthorizer(
		env.mapper,
		env.config,
		webhook.NewClient(http.DefaultClient),
		NewLimiter(func(r int) rate.Limiter { return rate.NewLocalRateLimiter(xrate.Limit(r)) }, 10, 5),
		10,
		env.subsidizerKey,
		env.mint,
	)
	require.NoError(t, err)

	return env
}

func TestAuthorizer_BadParse(t *testing.T) {
	env := setup(t)

	keys := testutil.GenerateSolanaKeys(t, 4)

	create, _, err := token.CreateAssociatedTokenAccount(
		keys[0],
		keys[1],
		env.mint,
	)
	require.NoError(t, err)

	cases := []solana.Transaction{
		// two memos
		solana.NewTransaction(
			keys[0],
			memo.Instruction("1-test"),
			memo.Instruction("1-test"),
			create,
		),
		// two creates
		solana.NewTransaction(
			keys[0],
			create,
			create,
		),
		// no creates
		solana.NewTransaction(
			keys[0],
			memo.Instruction("1-test"),
			token.Transfer(keys[1], keys[2], keys[3], 10),
		),
		// extra transfer
		solana.NewTransaction(
			keys[0],
			memo.Instruction("1-test"),
			create,
			token.Transfer(
				keys[1],
				keys[2],
				keys[3],
				10,
			),
		),
		// create without init
		solana.NewTransaction(
			keys[0],
			system.CreateAccount(
				keys[0],
				keys[1],
				keys[2],
				10,
				token.AccountSize,
			),
		),
		// init without create
		solana.NewTransaction(
			keys[0],
			token.InitializeAccount(
				keys[0],
				keys[1],
				keys[2],
			),
		),
		// invalid lamports
		solana.NewTransaction(
			keys[0],
			system.CreateAccount(
				keys[0],
				keys[1],
				keys[2],
				11,
				token.AccountSize,
			),
			token.InitializeAccount(
				keys[1],
				env.mint,
				keys[3],
			),
		),
		// invalid size
		solana.NewTransaction(
			keys[0],
			system.CreateAccount(
				keys[0],
				keys[1],
				keys[2],
				10,
				1,
			),
			token.InitializeAccount(
				keys[1],
				env.mint,
				keys[3],
			),
		),
		// invalid mint
		solana.NewTransaction(
			keys[0],
			system.CreateAccount(
				keys[0],
				keys[1],
				keys[2],
				10,
				token.AccountSize,
			),
			token.InitializeAccount(
				keys[1],
				keys[2],
				keys[3],
			),
		),
	}

	for c := range cases {
		_, err := env.auth.Authorize(env.ctx, cases[c])
		assert.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	}
}

func TestAuthorizer_SetAuthority(t *testing.T) {
	env := setup(t)
	owner := testutil.GenerateSolanaKeys(t, 1)[0]

	assocCreate, account, err := token.CreateAssociatedTokenAccount(
		env.subsidizer,
		owner,
		env.mint,
	)
	require.NoError(t, err)

	createInstructions := [][]solana.Instruction{
		{
			assocCreate,
		},
		{
			system.CreateAccount(
				env.subsidizer,
				account,
				token.ProgramKey,
				10,
				token.AccountSize,
			),
			token.InitializeAccount(
				account,
				env.mint,
				owner,
			),
		},
	}

	for _, c := range createInstructions {
		correct := solana.NewTransaction(
			env.subsidizer,
			append(c, token.SetAuthority(
				account,
				owner,
				env.subsidizer,
				token.AuthorityTypeCloseAccount,
			))...,
		)

		result, err := env.auth.Authorize(env.ctx, correct)
		assert.NoError(t, err)
		assert.Equal(t, AuthorizationResultOK, result.Result)
		assert.Equal(t, assocCreate.Accounts[1].PublicKey, result.Address)
		assert.Equal(t, owner, result.Owner)
		assert.Equal(t, env.subsidizer, result.CloseAuthority)

		missing := solana.NewTransaction(
			env.subsidizer,
			c...,
		)

		result, err = env.auth.Authorize(env.ctx, missing)
		assert.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		assert.True(t, strings.Contains(err.Error(), "missing SplToken::SetAuthority"), err.Error())

		missingCorruptedSig := solana.NewTransaction(
			env.subsidizer,
			c...,
		)
		_, err = rand.Read(missingCorruptedSig.Signatures[0][:])
		require.NoError(t, err)

		result, err = env.auth.Authorize(env.ctx, missing)
		assert.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
		assert.True(t, strings.Contains(err.Error(), "missing SplToken::SetAuthority"), err.Error())
	}

	incorrectSetAuths := []solana.Instruction{
		// Wrong account
		token.SetAuthority(
			testutil.GenerateSolanaKeys(t, 1)[0],
			owner,
			env.subsidizer,
			token.AuthorityTypeCloseAccount,
		),
		// Wrong owner
		token.SetAuthority(
			account,
			account,
			env.subsidizer,
			token.AuthorityTypeCloseAccount,
		),
		// Wrong subsidizer
		token.SetAuthority(
			account,
			owner,
			owner,
			token.AuthorityTypeCloseAccount,
		),
		// Wrong auth type
		token.SetAuthority(
			account,
			owner,
			env.subsidizer,
			token.AuthorityTypeFreezeAccount,
		),
	}

	for _, c := range createInstructions {
		for _, i := range incorrectSetAuths {
			assoc := solana.NewTransaction(
				env.subsidizer,
				append(c, i)...,
			)
			_, err := env.auth.Authorize(env.ctx, assoc)
			assert.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
		}
	}
}

func TestAuthorizer_RateLimiter(t *testing.T) {
	env := setup(t)

	require.NoError(t, env.mapper.Add(context.Background(), "one", 1))

	owner := testutil.GenerateSolanaKeys(t, 1)[0]
	create, account, err := token.CreateAssociatedTokenAccount(
		env.subsidizer,
		owner,
		env.mint,
	)
	require.NoError(t, err)

	tx := solana.NewTransaction(
		env.subsidizer,
		memo.Instruction("1-one"),
		create,
		token.SetAuthority(
			account,
			owner,
			env.subsidizer,
			token.AuthorityTypeCloseAccount,
		),
	)

	// Hit app limit
	for i := 0; i < 5; i++ {
		result, err := env.auth.Authorize(env.ctx, tx)
		assert.NoError(t, err)
		assert.Equal(t, AuthorizationResultOK, result.Result)
		assert.NotEmpty(t, result.Signature)

		tx.Signatures[0] = solana.Signature{}
	}

	// Ensure limit hit
	_, err = env.auth.Authorize(env.ctx, tx)
	assert.Error(t, err)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))

	// Ensure another app isn't effected
	m, err := kin.NewMemo(1, kin.TransactionTypeNone, 2, make([]byte, 19))
	require.NoError(t, err)

	tx = solana.NewTransaction(
		env.subsidizer,
		memo.Instruction(base64.StdEncoding.EncodeToString(m[:])),
		create,
		token.SetAuthority(
			account,
			owner,
			env.subsidizer,
			token.AuthorityTypeCloseAccount,
		),
	)

	for i := 0; i < 5; i++ {
		result, err := env.auth.Authorize(env.ctx, tx)
		assert.NoError(t, err)
		assert.Equal(t, AuthorizationResultOK, result.Result)
		assert.NotEmpty(t, result.Signature)

		tx.Signatures[0] = solana.Signature{}
	}

	// Ensure limit hit
	_, err = env.auth.Authorize(env.ctx, tx)
	assert.Error(t, err)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))

	// Ensure global limit
	tx = solana.NewTransaction(
		env.subsidizer,
		memo.Instruction("1-other"),
		create,
		token.SetAuthority(
			account,
			owner,
			env.subsidizer,
			token.AuthorityTypeCloseAccount,
		),
	)

	// Ensure global limit hit
	_, err = env.auth.Authorize(env.ctx, tx)
	assert.Error(t, err)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
}

func TestAuthorizer_Webhook_200(t *testing.T) {
	env := setup(t)

	var callCount int64
	resp := &createaccount.SuccessResponse{}
	b, err := json.Marshal(resp)
	require.NoError(t, err)
	testServer := newTestServerWithJSONResponse(t, http.StatusOK, b, &callCount)

	// setup webhook mapping
	createAccountURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	require.NoError(t, env.config.Add(env.ctx, 1, &app.Config{
		AppName:          "myapp",
		CreateAccountURL: createAccountURL,
		WebhookSecret:    generateWebhookSecret(t),
	}))

	addr, owner, tx := generateCreate(t, env.subsidizer, env.mint, 1)
	result, err := env.auth.Authorize(env.ctx, tx)
	assert.NoError(t, err)
	assert.Equal(t, AuthorizationResultOK, result.Result)
	assert.Equal(t, addr, result.Address)
	assert.Equal(t, owner, result.Owner)
	assert.Equal(t, env.subsidizer, result.CloseAuthority)
	assert.Equal(t, ed25519.Sign(env.subsidizerKey, tx.Message.Marshal()), result.Signature)
	assert.EqualValues(t, 1, atomic.LoadInt64(&callCount))
}

func TestAuthorizer_Webhook_403(t *testing.T) {
	env := setup(t)

	var callCount int64
	resp := &struct{}{}
	b, err := json.Marshal(resp)
	require.NoError(t, err)
	testServer := newTestServerWithJSONResponse(t, http.StatusForbidden, b, &callCount)

	// setup webhook mapping
	createAccountURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	require.NoError(t, env.config.Add(env.ctx, 1, &app.Config{
		AppName:          "myapp",
		CreateAccountURL: createAccountURL,
		WebhookSecret:    generateWebhookSecret(t),
	}))

	addr, owner, tx := generateCreate(t, env.subsidizer, env.mint, 1)
	result, err := env.auth.Authorize(env.ctx, tx)
	assert.NoError(t, err)
	assert.Equal(t, AuthorizationResultPayerRequired, result.Result)
	assert.Equal(t, addr, result.Address)
	assert.Equal(t, owner, result.Owner)
	assert.Equal(t, env.subsidizer, result.CloseAuthority)
	assert.Equal(t, ed25519.Sign(env.subsidizerKey, tx.Message.Marshal()), result.Signature)
	assert.EqualValues(t, 1, atomic.LoadInt64(&callCount))
}

func TestAuthorizer_Webhook_NoSignature(t *testing.T) {
	env := setup(t)

	var callCount int64
	resp := &createaccount.SuccessResponse{}
	b, err := json.Marshal(resp)
	require.NoError(t, err)
	testServer := newTestServerWithJSONResponse(t, http.StatusOK, b, &callCount)

	// setup webhook mapping
	createAccountURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	require.NoError(t, env.config.Add(env.ctx, 1, &app.Config{
		AppName:          "myapp",
		CreateAccountURL: createAccountURL,
		WebhookSecret:    generateWebhookSecret(t),
	}))

	subsidizer := testutil.GenerateSolanaKeys(t, 1)[0]
	addr, owner, tx := generateCreate(t, subsidizer, env.mint, 1)

	result, err := env.auth.Authorize(env.ctx, tx)
	assert.NoError(t, err)
	assert.Equal(t, AuthorizationResultPayerRequired, result.Result)
	assert.Equal(t, addr, result.Address)
	assert.Equal(t, owner, result.Owner)
	assert.Equal(t, subsidizer, result.CloseAuthority)
	assert.Empty(t, result.Signature)
	assert.EqualValues(t, 1, atomic.LoadInt64(&callCount))
}

func TestAuthorizer_Webhook_Subsidized(t *testing.T) {
	env := setup(t)

	subsidizer := testutil.GenerateSolanaKeypair(t)
	addr, owner, tx := generateCreate(t, subsidizer.Public().(ed25519.PublicKey), env.mint, 1)

	var callCount int64
	resp := &createaccount.SuccessResponse{
		Signature: ed25519.Sign(subsidizer, tx.Message.Marshal()),
	}
	b, err := json.Marshal(resp)
	require.NoError(t, err)
	testServer := newTestServerWithJSONResponse(t, http.StatusOK, b, &callCount)

	// setup webhook mapping
	createAccountURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	require.NoError(t, env.config.Add(env.ctx, 1, &app.Config{
		AppName:          "myapp",
		CreateAccountURL: createAccountURL,
		WebhookSecret:    generateWebhookSecret(t),
	}))

	result, err := env.auth.Authorize(env.ctx, tx)
	assert.NoError(t, err)
	assert.Equal(t, AuthorizationResultOK, result.Result)
	assert.Equal(t, addr, result.Address)
	assert.Equal(t, owner, result.Owner)
	assert.Equal(t, subsidizer.Public(), result.CloseAuthority)
	assert.Equal(t, ed25519.Sign(subsidizer, tx.Message.Marshal()), result.Signature)
	assert.Equal(t, ed25519.Sign(subsidizer, tx.Message.Marshal()), tx.Signatures[0][:])
	assert.EqualValues(t, 1, atomic.LoadInt64(&callCount))
}

func TestAuthorizer_Webhook_Subsidized_ImplicitAppIndex(t *testing.T) {
	env := setup(t)

	subsidizer := testutil.GenerateSolanaKeypair(t)
	addr, owner, tx := generateCreate(t, subsidizer.Public().(ed25519.PublicKey), env.mint, 0)

	var callCount int64
	resp := &createaccount.SuccessResponse{
		Signature: ed25519.Sign(subsidizer, tx.Message.Marshal()),
	}
	b, err := json.Marshal(resp)
	require.NoError(t, err)
	testServer := newTestServerWithJSONResponse(t, http.StatusOK, b, &callCount)

	// setup webhook mapping
	createAccountURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	require.NoError(t, env.config.Add(env.ctx, 1, &app.Config{
		AppName:          "myapp",
		CreateAccountURL: createAccountURL,
		WebhookSecret:    generateWebhookSecret(t),
	}))

	ctx := env.ctx
	require.NoError(t, headers.SetASCIIHeader(ctx, "app-index", "1"))

	result, err := env.auth.Authorize(ctx, tx)
	assert.NoError(t, err)
	assert.Equal(t, AuthorizationResultOK, result.Result)
	assert.Equal(t, addr, result.Address)
	assert.Equal(t, owner, result.Owner)
	assert.Equal(t, subsidizer.Public(), result.CloseAuthority)
	assert.Equal(t, ed25519.Sign(subsidizer, tx.Message.Marshal()), result.Signature)
	assert.Equal(t, ed25519.Sign(subsidizer, tx.Message.Marshal()), tx.Signatures[0][:])
	assert.EqualValues(t, 1, atomic.LoadInt64(&callCount))
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

func generateCreate(t *testing.T, subsidizer, mint ed25519.PublicKey, appIndex int) (ed25519.PublicKey, ed25519.PublicKey, solana.Transaction) {
	keys := testutil.GenerateSolanaKeys(t, 1)
	create, account, err := token.CreateAssociatedTokenAccount(
		subsidizer,
		keys[0],
		mint,
	)
	require.NoError(t, err)

	var instructions []solana.Instruction
	if appIndex > 0 {
		m, err := kin.NewMemo(1, kin.TransactionTypeNone, uint16(appIndex), make([]byte, 29))
		require.NoError(t, err)

		instructions = append(instructions, memo.Instruction(base64.StdEncoding.EncodeToString(m[:])))
	}

	instructions = append(instructions, create)
	instructions = append(instructions, token.SetAuthority(
		account,
		keys[0],
		subsidizer,
		token.AuthorityTypeCloseAccount,
	))

	return create.Accounts[1].PublicKey, keys[0], solana.NewTransaction(
		subsidizer,
		instructions...,
	)
}
