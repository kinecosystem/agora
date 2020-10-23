package transaction

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/agora-common/headers"
	"github.com/kinecosystem/agora-common/kin"
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
	"github.com/kinecosystem/agora/pkg/version"
	"github.com/kinecosystem/agora/pkg/webhook"
	"github.com/kinecosystem/agora/pkg/webhook/signtransaction"
)

type testEnv struct {
	ctx            context.Context
	auth           Authorizer
	appConfigStore app.ConfigStore
	appMapper      app.Mapper
}

func setup(t *testing.T) (env testEnv) {
	var err error

	env.appConfigStore = appconfigdb.New()
	env.appMapper = appmapper.New()
	env.auth, err = NewAuthorizer(
		env.appMapper,
		env.appConfigStore,
		webhook.NewClient(http.DefaultClient),
		NewLimiter(func(r int) rate.Limiter { return rate.NewLocalRateLimiter(xrate.Limit(r)) }, 10, 5),
	)
	require.NoError(t, err)
	env.ctx, err = headers.ContextWithHeaders(context.Background())
	require.NoError(t, err)

	return env
}

func TestAuthorizer_NoMemo(t *testing.T) {
	env := setup(t)

	result, err := env.auth.Authorize(context.Background(), Transaction{
		Version: version.KinVersion4,
		ID:      make([]byte, 32),
		OpCount: 1,
	})
	assert.NoError(t, err)
	assert.Equal(t, AuthorizationResultOK, result.Result)
}

func TestAuthorizer_AppNotFound(t *testing.T) {
	env := setup(t)

	_, err := env.auth.Authorize(env.ctx, generateTransaction(t, 1, nil))
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestAuthorizer_AppSignURLNotSet(t *testing.T) {
	env := setup(t)

	err := env.appConfigStore.Add(context.Background(), 1, &app.Config{
		AppName: "some name",
	})
	require.NoError(t, err)

	memo, err := kin.NewMemo(1, kin.TransactionTypeSpend, 1, nil)
	assert.NoError(t, err)

	result, err := env.auth.Authorize(context.Background(), Transaction{
		Version: version.KinVersion4,
		ID:      make([]byte, 32),
		OpCount: 1,
		Memo: Memo{
			Memo: &memo,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, AuthorizationResultOK, result.Result)
}

func TestAuthorizer_SignTransaction200_WithInvoice(t *testing.T) {
	env := setup(t)
	// Set up test server with a successful sign response
	webhookResp := &signtransaction.SuccessResponse{EnvelopeXDR: make([]byte, 12)}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)
	testServer := newTestServerWithJSONResponse(t, 200, b)

	// Set test server URL to app config
	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	err = env.appConfigStore.Add(context.Background(), 1, &app.Config{
		AppName:            "some name",
		SignTransactionURL: signURL,
		WebhookSecret:      generateWebhookKey(t),
	})
	require.NoError(t, err)

	result, err := env.auth.Authorize(env.ctx, generateTransaction(t, 1, nil))
	assert.NoError(t, err)
	assert.Equal(t, AuthorizationResultOK, result.Result)
}

func TestAthorizer_EarnNoWebhook(t *testing.T) {
	env := setup(t)

	// Set up test server that fails all responses.
	//
	// Since we shouldn't be calling out for earns, this
	// shouldn't get hit.
	webhookResp := &signtransaction.SuccessResponse{EnvelopeXDR: make([]byte, 32)}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)
	testServer := newTestServerWithJSONResponse(t, 400, b)

	// Set test server URL to app config
	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	err = env.appConfigStore.Add(context.Background(), 1, &app.Config{
		AppName:            "some name",
		SignTransactionURL: signURL,
		WebhookSecret:      generateWebhookKey(t),
	})
	require.NoError(t, err)

	memo, err := kin.NewMemo(1, kin.TransactionTypeEarn, 1, nil)
	require.NoError(t, err)

	txn := generateTransaction(t, 1, nil)
	txn.Memo.Memo = &memo

	result, err := env.auth.Authorize(env.ctx, txn)
	assert.NoError(t, err)
	assert.Equal(t, AuthorizationResultOK, result.Result)
}

func TestAuthorizer_SignTransaction400(t *testing.T) {
	env := setup(t)

	// Set up test server with 400 response
	webhookResp := &signtransaction.BadRequestResponse{Message: "some message"}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)
	testServer := newTestServerWithJSONResponse(t, 400, b)

	// Set test server URL to app config
	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	err = env.appConfigStore.Add(context.Background(), 1, &app.Config{
		AppName:            "some name",
		SignTransactionURL: signURL,
		WebhookSecret:      generateWebhookKey(t),
	})
	require.NoError(t, err)

	memo, err := kin.NewMemo(1, kin.TransactionTypeSpend, 1, nil)
	assert.NoError(t, err)

	_, err = env.auth.Authorize(context.Background(), Transaction{
		Version: version.KinVersion4,
		ID:      make([]byte, 32),
		OpCount: 1,
		Memo: Memo{
			Memo: &memo,
		},
	})
	assert.Equal(t, codes.Internal, status.Code(err))
}

func TestAuthorizer_SignTransaction403(t *testing.T) {
	env := setup(t)

	// Set up test server with 403 response
	webhookResp := &signtransaction.ForbiddenResponse{
		Message: "some message",
	}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)
	testServer := newTestServerWithJSONResponse(t, 403, b)

	// Set test server URL to app config
	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	err = env.appConfigStore.Add(context.Background(), 1, &app.Config{
		AppName:            "some name",
		SignTransactionURL: signURL,
		WebhookSecret:      generateWebhookKey(t),
	})
	require.NoError(t, err)

	invoiceList := &commonpb.InvoiceList{
		Invoices: []*commonpb.Invoice{
			{
				Items: []*commonpb.Invoice_LineItem{
					{
						Title:       "1",
						Description: "desc1",
						Amount:      5,
					},
				},
			},
			{
				Items: []*commonpb.Invoice_LineItem{
					{
						Title:       "2",
						Description: "desc1",
						Amount:      10,
					},
				},
			},
			{
				Items: []*commonpb.Invoice_LineItem{
					{
						Title:       "3",
						Description: "desc1",
						Amount:      15,
					},
				},
			},
			{
				Items: []*commonpb.Invoice_LineItem{
					{
						Title:       "4",
						Description: "desc1",
						Amount:      20,
					},
				},
			},
		},
	}

	result, err := env.auth.Authorize(env.ctx, generateTransaction(t, 1, invoiceList))
	assert.NoError(t, err)
	assert.Equal(t, AuthorizationResultRejected, result.Result)
	assert.Equal(t, len(webhookResp.InvoiceErrors), len(result.InvoiceErrors))
}

func TestAuthorizer_SignTransaction403_InvoiceErrors(t *testing.T) {
	env := setup(t)

	// Set up test server with 403 response
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
	testServer := newTestServerWithJSONResponse(t, 403, b)

	// Set test server URL to app config
	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	err = env.appConfigStore.Add(context.Background(), 1, &app.Config{
		AppName:            "some name",
		SignTransactionURL: signURL,
		WebhookSecret:      generateWebhookKey(t),
	})
	require.NoError(t, err)

	invoiceList := &commonpb.InvoiceList{
		Invoices: []*commonpb.Invoice{
			{
				Items: []*commonpb.Invoice_LineItem{
					{
						Title:       "1",
						Description: "desc1",
						Amount:      5,
					},
				},
			},
			{
				Items: []*commonpb.Invoice_LineItem{
					{
						Title:       "2",
						Description: "desc1",
						Amount:      10,
					},
				},
			},
			{
				Items: []*commonpb.Invoice_LineItem{
					{
						Title:       "3",
						Description: "desc1",
						Amount:      15,
					},
				},
			},
			{
				Items: []*commonpb.Invoice_LineItem{
					{
						Title:       "4",
						Description: "desc1",
						Amount:      20,
					},
				},
			},
		},
	}

	resp, err := env.auth.Authorize(env.ctx, generateTransaction(t, 1, invoiceList))
	assert.Nil(t, err)

	assert.Equal(t, AuthorizationResultInvoiceError, resp.Result)
	assert.Equal(t, len(webhookResp.InvoiceErrors), len(resp.InvoiceErrors))

	assert.Equal(t, uint32(0), resp.InvoiceErrors[0].OpIndex)
	assert.Equal(t, commonpb.InvoiceError_ALREADY_PAID, resp.InvoiceErrors[0].Reason)
	assert.True(t, proto.Equal(invoiceList.Invoices[0], resp.InvoiceErrors[0].Invoice))

	assert.Equal(t, uint32(1), resp.InvoiceErrors[1].OpIndex)
	assert.Equal(t, commonpb.InvoiceError_WRONG_DESTINATION, resp.InvoiceErrors[1].Reason)
	assert.True(t, proto.Equal(invoiceList.Invoices[1], resp.InvoiceErrors[1].Invoice))

	assert.Equal(t, uint32(2), resp.InvoiceErrors[2].OpIndex)
	assert.Equal(t, commonpb.InvoiceError_SKU_NOT_FOUND, resp.InvoiceErrors[2].Reason)
	assert.True(t, proto.Equal(invoiceList.Invoices[2], resp.InvoiceErrors[2].Invoice))

	assert.Equal(t, uint32(3), resp.InvoiceErrors[3].OpIndex)
	assert.Equal(t, commonpb.InvoiceError_UNKNOWN, resp.InvoiceErrors[3].Reason)
	assert.True(t, proto.Equal(invoiceList.Invoices[3], resp.InvoiceErrors[3].Invoice))
}

func TestAuthorizer_InvalidInvoiceList(t *testing.T) {
	env := setup(t)

	// mismatch invoice and tx operation counts
	invalid := &commonpb.InvoiceList{
		Invoices: []*commonpb.Invoice{
			{
				Items: []*commonpb.Invoice_LineItem{
					{
						Title:  "lineitem1",
						Amount: 1,
					},
				},
			},
		},
	}

	txn := generateTransaction(t, 1, invalid)

	invalid.Invoices = append(invalid.Invoices,
		&commonpb.Invoice{
			Items: []*commonpb.Invoice_LineItem{
				{
					Title:  "lineitem2",
					Amount: 1,
				},
			},
		},
	)

	_, err := env.auth.Authorize(env.ctx, txn)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestAuthorizer_InvalidMemo(t *testing.T) {
	env := setup(t)

	invoiceList := &commonpb.InvoiceList{
		Invoices: []*commonpb.Invoice{
			{
				Items: []*commonpb.Invoice_LineItem{
					{
						Title:  "lineitem1",
						Amount: 1,
					},
				},
			},
		},
	}

	txn := generateTransaction(t, 1, invoiceList)

	memo, err := kin.NewMemo(1, kin.TransactionTypeEarn, 1, make([]byte, 29))
	assert.NoError(t, err)
	txn.Memo.Memo = &memo

	_, err = env.auth.Authorize(env.ctx, txn)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.True(t, strings.Contains(err.Error(), "fk did not match invoice list hash"), err.Error())
}

func TestAuthorizer_RateLimiter(t *testing.T) {
	env := setup(t)

	err := env.appConfigStore.Add(context.Background(), 1, &app.Config{
		AppName:       "some name",
		WebhookSecret: generateWebhookKey(t),
	})
	require.NoError(t, err)
	err = env.appConfigStore.Add(context.Background(), 2, &app.Config{
		AppName:       "some other name",
		WebhookSecret: generateWebhookKey(t),
	})
	require.NoError(t, err)
	err = env.appConfigStore.Add(context.Background(), 3, &app.Config{
		AppName:       "yet another name",
		WebhookSecret: generateWebhookKey(t),
	})
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		result, err := env.auth.Authorize(env.ctx, generateTransaction(t, 1, nil))
		assert.NoError(t, err)
		assert.Equal(t, AuthorizationResultOK, result.Result)
	}

	_, err = env.auth.Authorize(env.ctx, generateTransaction(t, 1, nil))
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))

	for i := 0; i < 5; i++ {
		result, err := env.auth.Authorize(env.ctx, generateTransaction(t, 2, nil))
		assert.NoError(t, err)
		assert.Equal(t, AuthorizationResultOK, result.Result)
	}

	_, err = env.auth.Authorize(env.ctx, generateTransaction(t, 2, nil))
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))

	// Global limit should have been hit, so no other request can go through
	_, err = env.auth.Authorize(env.ctx, generateTransaction(t, 3, nil))
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
}

func TestAuthorizer_TextMemo_NoAppID(t *testing.T) {
	env := setup(t)

	// We should still submit arbitrary text memos

	memo := "sometext"
	txn := generateTransaction(t, 0, nil)
	txn.Memo.Text = &memo

	result, err := env.auth.Authorize(env.ctx, txn)
	assert.NoError(t, err)
	assert.Equal(t, AuthorizationResultOK, result.Result)
}

func TestAuthorizer_TextMemo_AppID(t *testing.T) {
	env := setup(t)

	err := env.appMapper.Add(context.Background(), "test", 1)
	require.NoError(t, err)

	err = env.appConfigStore.Add(context.Background(), 1, &app.Config{
		AppName: "some name",
	})
	require.NoError(t, err)

	memo := "1-test"
	txn := generateTransaction(t, 0, nil)
	txn.Memo.Text = &memo

	result, err := env.auth.Authorize(env.ctx, txn)
	assert.NoError(t, err)
	assert.Equal(t, AuthorizationResultOK, result.Result)
}

func TestAuthorizer_TextMemo_NoMapping(t *testing.T) {
	env := setup(t)

	// If a text memo results in no mapping being found, it should still get submitted
	memo := "1-test"
	txn := generateTransaction(t, 0, nil)
	txn.Memo.Text = &memo

	result, err := env.auth.Authorize(env.ctx, txn)
	assert.NoError(t, err)
	assert.Equal(t, AuthorizationResultOK, result.Result)
}

func TestAuthorizer_TextMemo_NoConfig(t *testing.T) {
	env := setup(t)

	// If a mapping exists but the app has no config, we treat it as an invalid app
	require.NoError(t, env.appMapper.Add(context.Background(), "test", 1))

	memo := "1-test"
	txn := generateTransaction(t, 0, nil)
	txn.Memo.Text = &memo

	_, err := env.auth.Authorize(env.ctx, txn)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.True(t, strings.Contains(err.Error(), "app index not found"), err.Error())
}

func TestAppIDFromTextMemo(t *testing.T) {
	valid := []string{
		"1-tes",
		"1-test",
	}
	for _, m := range valid {
		appID, ok := AppIDFromTextMemo(m)
		assert.True(t, ok)
		assert.Equal(t, m[2:], appID)
	}

	invalid := []string{
		"",
		"1-te",
		"1-tests",
		"2-test",
	}

	for _, m := range invalid {
		_, ok := AppIDFromTextMemo(m)
		assert.False(t, ok)
	}
}

func newTestServerWithJSONResponse(t *testing.T, statusCode int, b []byte) *httptest.Server {
	testServer := httptest.NewServer(http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		resp.WriteHeader(statusCode)
		resp.Header().Set("Content-Type", "application/json")
		_, err := resp.Write(b)
		require.NoError(t, err)
	}))
	return testServer
}

func generateWebhookKey(t *testing.T) string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	require.NoError(t, err)
	return string(b)
}

func generateTransaction(t *testing.T, appIndex uint16, invoiceList *commonpb.InvoiceList) Transaction {
	txn := Transaction{
		Version:     version.KinVersion3,
		ID:          make([]byte, 32),
		InvoiceList: invoiceList,
		SignRequest: &signtransaction.RequestBody{
			KinVersion:  3,
			EnvelopeXDR: []byte("test"),
		},
	}
	if invoiceList != nil {
		txn.OpCount = len(invoiceList.Invoices)

		invoiceListBytes, err := proto.Marshal(invoiceList)
		require.NoError(t, err)
		txn.SignRequest.InvoiceList = invoiceListBytes

		invoiceListHash := sha256.Sum224(invoiceListBytes)
		memo, err := kin.NewMemo(1, kin.TransactionTypeSpend, appIndex, invoiceListHash[:])
		assert.NoError(t, err)
		txn.Memo.Memo = &memo

	} else {
		txn.OpCount = 1

		if appIndex > 0 {
			memo, err := kin.NewMemo(1, kin.TransactionTypeSpend, appIndex, make([]byte, 29))
			assert.NoError(t, err)
			txn.Memo.Memo = &memo
		}
	}

	return txn
}
