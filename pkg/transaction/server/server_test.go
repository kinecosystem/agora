package server

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/go-redis/redis_rate/v8"
	"github.com/golang/protobuf/proto"
	agoraenv "github.com/kinecosystem/agora-common/env"
	"github.com/kinecosystem/agora-common/headers"
	"github.com/kinecosystem/agora-common/kin"
	redistest "github.com/kinecosystem/agora-common/redis/test"
	agoratestutil "github.com/kinecosystem/agora-common/testutil"
	"github.com/kinecosystem/go/clients/horizon"
	"github.com/kinecosystem/go/keypair"
	"github.com/kinecosystem/go/network"
	horizonprotocols "github.com/kinecosystem/go/protocols/horizon"
	"github.com/kinecosystem/go/strkey"
	"github.com/kinecosystem/go/xdr"
	"github.com/ory/dockertest"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go/clients/horizonclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"
	transactionpb "github.com/kinecosystem/agora-api/genproto/transaction/v3"

	"github.com/kinecosystem/agora/pkg/app"
	appconfigdb "github.com/kinecosystem/agora/pkg/app/memory"
	appmapper "github.com/kinecosystem/agora/pkg/app/memory/mapper"
	"github.com/kinecosystem/agora/pkg/invoice"
	invoicedb "github.com/kinecosystem/agora/pkg/invoice/memory"
	"github.com/kinecosystem/agora/pkg/testutil"
	"github.com/kinecosystem/agora/pkg/transaction"
	"github.com/kinecosystem/agora/pkg/transaction/history"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
	ingestionmemory "github.com/kinecosystem/agora/pkg/transaction/history/ingestion/memory"
	historymemory "github.com/kinecosystem/agora/pkg/transaction/history/memory"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
	historytestutil "github.com/kinecosystem/agora/pkg/transaction/history/model/testutil"
	"github.com/kinecosystem/agora/pkg/webhook"
	"github.com/kinecosystem/agora/pkg/webhook/signtransaction"
)

var (
	redisConnString string

	il = &commonpb.InvoiceList{
		Invoices: []*commonpb.Invoice{
			{
				Items: []*commonpb.Invoice_LineItem{
					{
						Title:       "lineitem1",
						Description: "desc1",
						Amount:      5,
					},
				},
			},
		},
	}
)

type testEnv struct {
	client transactionpb.TransactionClient

	hClient   *horizon.MockClient
	hClientV2 *horizonclient.MockClient

	appConfigStore app.ConfigStore
	appMapper      app.Mapper
	invoiceStore   invoice.Store
	rw             history.ReaderWriter
	committer      ingestion.Committer
}

func TestMain(m *testing.M) {
	log := logrus.StandardLogger()

	pool, err := dockertest.NewPool("")
	if err != nil {
		log.WithError(err).Error("Error creating docker pool")
		os.Exit(1)
	}

	var cleanUpFunc func()
	redisConnString, cleanUpFunc, err = redistest.StartRedis(context.Background(), pool)
	if err != nil {
		log.WithError(err).Error("Error starting redis connection")
		os.Exit(1)
	}

	code := m.Run()
	cleanUpFunc()
	os.Exit(code)
}

func setup(t *testing.T, submitTxGlobalRL, submitTxAppRL int) (env testEnv, cleanup func()) {
	os.Setenv("AGORA_ENVIRONMENT", string(agoraenv.AgoraEnvironmentDev))

	conn, serv, err := agoratestutil.NewServer(
		agoratestutil.WithUnaryServerInterceptor(headers.UnaryServerInterceptor()),
		agoratestutil.WithStreamServerInterceptor(headers.StreamServerInterceptor()),
	)
	require.NoError(t, err)

	env.client = transactionpb.NewTransactionClient(conn)
	env.hClient = &horizon.MockClient{}
	env.hClientV2 = &horizonclient.MockClient{}

	env.appConfigStore = appconfigdb.New()
	env.appMapper = appmapper.New()
	env.invoiceStore = invoicedb.New()

	env.rw = historymemory.New()
	env.committer = ingestionmemory.New()

	ring := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{
			"server1": redisConnString,
		},
	})
	limiter := redis_rate.NewLimiter(ring)
	// reset global & app rate limits (the key format is hardcoded inside redis_rate/rate.go)
	ring.Del(
		fmt.Sprintf("rate:%s", globalRateLimitKey),
		fmt.Sprintf("rate:%s", fmt.Sprintf(appRateLimitKeyFormat, 0)),
		fmt.Sprintf("rate:%s", fmt.Sprintf(appRateLimitKeyFormat, 1)),
	)

	s, err := New(
		env.appConfigStore,
		env.appMapper,
		env.invoiceStore,
		env.rw,
		env.committer,
		env.hClient,
		env.hClientV2,
		webhook.NewClient(http.DefaultClient),
		limiter,
		&Config{
			SubmitTxGlobalLimit: submitTxGlobalRL,
			SubmitTxAppLimit:    submitTxAppRL,
		},
	)
	require.NoError(t, err)
	serv.RegisterService(func(server *grpc.Server) {
		transactionpb.RegisterTransactionServer(server, s)
	})

	cleanup, err = serv.Serve()
	require.NoError(t, err)

	return env, cleanup
}

func TestSubmitTransaction_NoKinMemo(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

	_, envelopeBytes, txHash := generateEnvelope(t, nil, 0)

	horizonResult := horizonprotocols.TransactionSuccess{
		Hash:   hex.EncodeToString(txHash),
		Ledger: 10,
		Result: base64.StdEncoding.EncodeToString([]byte("test")),
	}
	env.hClient.On("SubmitTransaction", mock.AnythingOfType("string")).Return(horizonResult, nil).Once()
	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		EnvelopeXdr: envelopeBytes,
	})

	require.NoError(t, err)
	assert.EqualValues(t, horizonResult.Ledger, resp.Ledger)
	assert.EqualValues(t, horizonResult.Hash, hex.EncodeToString(resp.Hash.Value))
	assert.EqualValues(t, horizonResult.Result, base64.StdEncoding.EncodeToString(resp.ResultXdr))
	require.Len(t, env.hClient.Calls, 1)

	var submittedEnvelope xdr.TransactionEnvelope
	submittedEnvelopeBytes, err := base64.StdEncoding.DecodeString(env.hClient.Calls[0].Arguments[0].(string))
	require.NoError(t, err)
	require.NoError(t, submittedEnvelope.UnmarshalBinary(submittedEnvelopeBytes))
	require.Len(t, submittedEnvelope.Signatures, 1)
}

func TestSubmitTransaction_AppNotFound(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

	_, envelopeBytes, _ := generateEnvelope(t, il, 1)
	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		EnvelopeXdr: envelopeBytes,
		InvoiceList: il,
	})

	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Nil(t, resp)
}

func TestSubmitTransaction_AppSignTxURLNotSet(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

	err := env.appConfigStore.Add(context.Background(), 1, &app.Config{
		AppName:          "some name",
		InvoicingEnabled: true,
	})
	require.NoError(t, err)

	_, envelopeBytes, _ := generateEnvelope(t, il, 1)
	hashBytes := sha256.Sum256(envelopeBytes)
	horizonResult := horizonprotocols.TransactionSuccess{
		Hash:   hex.EncodeToString(hashBytes[:]),
		Ledger: 10,
		Result: base64.StdEncoding.EncodeToString([]byte("test")),
	}
	env.hClient.On("SubmitTransaction", mock.AnythingOfType("string")).Return(horizonResult, nil).Once()

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		EnvelopeXdr: envelopeBytes,
		InvoiceList: il,
	})

	assert.NoError(t, err)
	assert.EqualValues(t, horizonResult.Ledger, resp.Ledger)
	assert.EqualValues(t, horizonResult.Hash, hex.EncodeToString(resp.Hash.Value))
	assert.EqualValues(t, horizonResult.Result, base64.StdEncoding.EncodeToString(resp.ResultXdr))
}

func TestSubmitTransaction_SignTransaction400(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

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
		InvoicingEnabled:   false,
		WebhookSecret:      generateWebhookKey(t),
	})
	require.NoError(t, err)

	_, envelopeBytes, _ := generateEnvelope(t, il, 1)
	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		EnvelopeXdr: envelopeBytes,
	})

	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Nil(t, resp)
}

func TestSubmitTransaction_SignTransaction403_Rejected(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

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
		InvoicingEnabled:   false,
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

	_, envelopeBytes, _ := generateEnvelope(t, invoiceList, 1)

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		EnvelopeXdr: envelopeBytes,
		InvoiceList: invoiceList,
	})
	require.NoError(t, err)

	assert.Equal(t, transactionpb.SubmitTransactionResponse_REJECTED, resp.Result)
	assert.Equal(t, len(webhookResp.InvoiceErrors), len(resp.InvoiceErrors))
}

func TestSubmitTransaction_SignTransaction403_InvoiceError(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

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
		InvoicingEnabled:   false,
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

	_, envelopeBytes, _ := generateEnvelope(t, invoiceList, 1)

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		EnvelopeXdr: envelopeBytes,
		InvoiceList: invoiceList,
	})
	require.NoError(t, err)

	assert.Equal(t, transactionpb.SubmitTransactionResponse_INVOICE_ERROR, resp.Result)
	assert.Equal(t, len(webhookResp.InvoiceErrors), len(resp.InvoiceErrors))

	assert.Equal(t, uint32(0), resp.InvoiceErrors[0].OpIndex)
	assert.Equal(t, transactionpb.SubmitTransactionResponse_InvoiceError_ALREADY_PAID, resp.InvoiceErrors[0].Reason)
	assert.True(t, proto.Equal(invoiceList.Invoices[0], resp.InvoiceErrors[0].Invoice))

	assert.Equal(t, uint32(1), resp.InvoiceErrors[1].OpIndex)
	assert.Equal(t, transactionpb.SubmitTransactionResponse_InvoiceError_WRONG_DESTINATION, resp.InvoiceErrors[1].Reason)
	assert.True(t, proto.Equal(invoiceList.Invoices[1], resp.InvoiceErrors[1].Invoice))

	assert.Equal(t, uint32(2), resp.InvoiceErrors[2].OpIndex)
	assert.Equal(t, transactionpb.SubmitTransactionResponse_InvoiceError_SKU_NOT_FOUND, resp.InvoiceErrors[2].Reason)
	assert.True(t, proto.Equal(invoiceList.Invoices[2], resp.InvoiceErrors[2].Invoice))

	assert.Equal(t, uint32(3), resp.InvoiceErrors[3].OpIndex)
	assert.Equal(t, transactionpb.SubmitTransactionResponse_InvoiceError_UNKNOWN, resp.InvoiceErrors[3].Reason)
	assert.True(t, proto.Equal(invoiceList.Invoices[3], resp.InvoiceErrors[3].Invoice))
}

func TestSubmitTransaction_SignTransaction200WithInvoice(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

	_, envelopeBytes, txHash := generateEnvelope(t, il, 1)

	// Set up test server with a successful sign response
	webhookResp := &signtransaction.SuccessResponse{EnvelopeXDR: envelopeBytes}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)
	testServer := newTestServerWithJSONResponse(t, 200, b)

	// Set test server URL to app config
	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	err = env.appConfigStore.Add(context.Background(), 1, &app.Config{
		AppName:            "some name",
		SignTransactionURL: signURL,
		InvoicingEnabled:   true,
		WebhookSecret:      generateWebhookKey(t),
	})
	require.NoError(t, err)

	hashBytes := sha256.Sum256(envelopeBytes)
	horizonResult := horizonprotocols.TransactionSuccess{
		Hash:   hex.EncodeToString(hashBytes[:]),
		Ledger: 10,
		Result: base64.StdEncoding.EncodeToString([]byte("test")),
	}
	env.hClient.On("SubmitTransaction", mock.AnythingOfType("string")).Return(horizonResult, nil).Once()

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		EnvelopeXdr: envelopeBytes,
		InvoiceList: il,
	})
	require.NoError(t, err)

	assert.EqualValues(t, horizonResult.Ledger, resp.Ledger)
	assert.EqualValues(t, horizonResult.Hash, hex.EncodeToString(resp.Hash.Value))
	assert.EqualValues(t, horizonResult.Result, base64.StdEncoding.EncodeToString(resp.ResultXdr))

	// Ensure that the invoice got stored
	storedIL, err := env.invoiceStore.Get(context.Background(), txHash)
	require.NoError(t, err)
	assert.True(t, proto.Equal(il, storedIL))
}

func TestSubmitTransaction_SignTransaction200WithInvoiceAndDisabled(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

	_, envelopeBytes, txHash := generateEnvelope(t, il, 1)

	// Set up test server with a successful sign response
	webhookResp := &signtransaction.SuccessResponse{EnvelopeXDR: base64.StdEncoding.EncodeToString(envelopeBytes)}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)
	testServer := newTestServerWithJSONResponse(t, 200, b)

	// Set test server URL to app config
	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	err = env.appConfigStore.Add(context.Background(), 1, &app.Config{
		AppName:            "some name",
		SignTransactionURL: signURL,
		InvoicingEnabled:   false,
		WebhookSecret:      generateWebhookKey(t),
	})
	require.NoError(t, err)

	hashBytes := sha256.Sum256(envelopeBytes)
	horizonResult := horizonprotocols.TransactionSuccess{
		Hash:   hex.EncodeToString(hashBytes[:]),
		Ledger: 10,
		Result: base64.StdEncoding.EncodeToString([]byte("test")),
	}
	env.hClient.On("SubmitTransaction", mock.AnythingOfType("string")).Return(horizonResult, nil).Once()

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		EnvelopeXdr: envelopeBytes,
		InvoiceList: il,
	})
	require.NoError(t, err)

	assert.EqualValues(t, horizonResult.Ledger, resp.Ledger)
	assert.EqualValues(t, horizonResult.Hash, hex.EncodeToString(resp.Hash.Value))
	assert.EqualValues(t, horizonResult.Result, base64.StdEncoding.EncodeToString(resp.ResultXdr))

	// Ensure no invoice got stored
	_, err = env.invoiceStore.Get(context.Background(), txHash)
	require.Error(t, err, invoice.ErrNotFound)
}

func TestSubmitTransaction_SignTransaction200InvalidResponse(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

	_, envelopeBytes, _ := generateEnvelope(t, il, 1)

	// Set up test server with a successful sign response
	webhookResp := &signtransaction.SuccessResponse{EnvelopeXDR: []byte("invalidxdr")}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)
	testServer := newTestServerWithJSONResponse(t, 200, b)

	// Set test server URL to app config
	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	err = env.appConfigStore.Add(context.Background(), 1, &app.Config{
		AppName:            "some name",
		SignTransactionURL: signURL,
		InvoicingEnabled:   false,
		WebhookSecret:      generateWebhookKey(t),
	})
	require.NoError(t, err)

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		EnvelopeXdr: envelopeBytes,
	})

	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Nil(t, resp)
}

func TestSubmitTransaction_SignTransactionError(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

	_, envelopeBytes, _ := generateEnvelope(t, il, 1)

	// Set up test server with a successful sign response
	webhookResp := &signtransaction.SuccessResponse{EnvelopeXDR: []byte("invalidxdr")}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)
	testServer := newTestServerWithJSONResponse(t, 200, b)

	// Set test server URL to app config
	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	// Create an app config which is missing a webhook secret, which should cause an error when signing the transaction
	err = env.appConfigStore.Add(context.Background(), 1, &app.Config{
		AppName:            "some name",
		SignTransactionURL: signURL,
		InvoicingEnabled:   false,
	})
	require.NoError(t, err)

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		EnvelopeXdr: envelopeBytes,
	})

	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Nil(t, resp)
}

func TestSubmitTransaction_InvalidInvoiceList(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

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

	_, envelopeBytes, _ := generateEnvelope(t, invalid, 1)

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
	_, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		EnvelopeXdr: envelopeBytes,
		InvoiceList: invalid,
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestSubmitTransaction_WithInvoiceInvalidMemo(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

	envelope, _, _ := generateEnvelope(t, nil, 1)

	// wrong fk in memo
	wrongTxn := envelope.Tx
	wrongBytes := sha256.Sum256([]byte("somedata"))
	memo, err := kin.NewMemo(byte(0), kin.TransactionTypeSpend, 1, wrongBytes[:29])
	require.NoError(t, err)

	xdrHash := xdr.Hash{}
	for i := 0; i < len(memo); i++ {
		xdrHash[i] = memo[i]
	}
	xdrMemo, err := xdr.NewMemo(xdr.MemoTypeMemoHash, xdrHash)
	require.NoError(t, err)

	wrongTxn.Memo = xdrMemo
	envelopeBytes, err := wrongTxn.MarshalBinary()
	require.NoError(t, err)

	_, err = env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		EnvelopeXdr: envelopeBytes,
		InvoiceList: il,
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestSubmitTransaction_Invalid(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

	invalidRequests := []*transactionpb.SubmitTransactionRequest{
		{},
		{
			EnvelopeXdr: []byte{1, 2},
		},
	}

	/*
		todo: when we do memo verification, uncomment this
		m, err := kin.NewMemo(2, kin.TransactionTypeSpend, 0, make([]byte, 29))
		require.NoError(t, err)
		txn := emptyTxn
		txn.Memo.Type = xdr.MemoTypeMemoHash
		h := xdr.Hash(m)
		txn.Memo.Hash = &h
		txnEnvelopeBytes, err := txn.MarshalBinary()
		require.NoError(t, err)
		invalidRequests = append(invalidRequests, &transactionpb.SubmitTransactionRequest{
			EnvelopeXdr: txnEnvelopeBytes,
		})
	*/

	for _, r := range invalidRequests {
		_, err := env.client.SubmitTransaction(context.Background(), r)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	}
}

func TestSubmitTransaction_HorizonErrors(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

	_, envelopeBytes, _ := generateEnvelope(t, nil, 1)

	type testCase struct {
		hError horizon.Error
	}

	resultBytes, err := xdr.TransactionResult{Result: xdr.TransactionResultResult{Code: xdr.TransactionResultCodeTxBadSeq}}.MarshalBinary()
	require.NoError(t, err)

	testCases := []testCase{
		{
			hError: horizon.Error{
				Problem: horizon.Problem{
					Status: 500,
					Extras: map[string]json.RawMessage{
						"result_xdr": json.RawMessage(fmt.Sprintf("\"%s\"", base64.StdEncoding.EncodeToString(resultBytes))),
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		env.hClient.On("SubmitTransaction", mock.AnythingOfType("string")).Return(horizonprotocols.TransactionSuccess{}, error(&tc.hError)).Once()
		resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
			EnvelopeXdr: envelopeBytes,
		})
		require.NoError(t, err)
		require.Equal(t, transactionpb.SubmitTransactionResponse_FAILED, resp.Result)
		require.Equal(t, resultBytes, resp.ResultXdr)
	}
}

func TestSubmitTransaction_GlobalRateLimited(t *testing.T) {
	env, cleanup := setup(t, 5, -1)
	defer cleanup()

	var err error
	for i := uint16(0); i < 5; i++ {
		_, envelopeBytes, _ := generateEnvelope(t, nil, i)
		hashBytes := sha256.Sum256(envelopeBytes)
		horizonResult := horizonprotocols.TransactionSuccess{
			Hash:   hex.EncodeToString(hashBytes[:]),
			Ledger: 10,
			Result: base64.StdEncoding.EncodeToString([]byte("test")),
		}
		env.hClient.On("SubmitTransaction", mock.AnythingOfType("string")).Return(horizonResult, nil).Once()
		_, err = env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
			EnvelopeXdr: envelopeBytes,
			InvoiceList: il,
		})
		require.NoError(t, err)
	}

	_, envelopeBytes, _ := generateEnvelope(t, il, 1)
	_, err = env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		EnvelopeXdr: envelopeBytes,
		InvoiceList: il,
	})

	require.Equal(t, codes.Unavailable, status.Code(err))

	// wait until the rate limit resets
	time.Sleep(1 * time.Second)

	for i := uint16(0); i < 5; i++ {
		_, envelopeBytes, _ := generateEnvelope(t, nil, i)
		hashBytes := sha256.Sum256(envelopeBytes)
		horizonResult := horizonprotocols.TransactionSuccess{
			Hash:   hex.EncodeToString(hashBytes[:]),
			Ledger: 10,
			Result: base64.StdEncoding.EncodeToString([]byte("test")),
		}
		env.hClient.On("SubmitTransaction", mock.AnythingOfType("string")).Return(horizonResult, nil).Once()
		_, err = env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
			EnvelopeXdr: envelopeBytes,
			InvoiceList: il,
		})
		require.NoError(t, err)
	}

	_, envelopeBytes, _ = generateEnvelope(t, il, 1)
	_, err = env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		EnvelopeXdr: envelopeBytes,
		InvoiceList: il,
	})

	require.Equal(t, codes.Unavailable, status.Code(err))
	env.hClient.AssertExpectations(t)
}

func TestSubmitTransaction_AppRateLimited(t *testing.T) {
	env, cleanup := setup(t, -1, 3)
	defer cleanup()

	envelope, _, _ := generateEnvelope(t, nil, 1)

	// a memo is required for the app index rate limit
	memo, err := kin.NewMemo(byte(0), kin.TransactionTypeSpend, 1, make([]byte, 0))
	require.NoError(t, err)
	xdrHash := xdr.Hash{}
	for i := 0; i < len(memo); i++ {
		xdrHash[i] = memo[i]
	}
	xdrMemo, err := xdr.NewMemo(xdr.MemoTypeMemoHash, xdrHash)
	require.NoError(t, err)

	envelope.Tx.Memo = xdrMemo

	envelopeBytes, err := envelope.MarshalBinary()
	require.NoError(t, err)

	// Set up test server with a successful sign response
	webhookResp := &signtransaction.SuccessResponse{EnvelopeXDR: envelopeBytes}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)
	testServer := newTestServerWithJSONResponse(t, 200, b)

	// Set test server URL to app config
	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	err = env.appConfigStore.Add(context.Background(), 1, &app.Config{
		AppName:            "some name",
		SignTransactionURL: signURL,
		InvoicingEnabled:   true,
		WebhookSecret:      generateWebhookKey(t),
	})
	require.NoError(t, err)

	hashBytes := sha256.Sum256(envelopeBytes)
	horizonResult := horizonprotocols.TransactionSuccess{
		Hash:   hex.EncodeToString(hashBytes[:]),
		Ledger: 10,
		Result: base64.StdEncoding.EncodeToString([]byte("test")),
	}
	env.hClient.On("SubmitTransaction", mock.AnythingOfType("string")).Return(horizonResult, nil).Times(6)

	for i := 0; i < 3; i++ {
		_, err = env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
			EnvelopeXdr: envelopeBytes,
		})
		require.NoError(t, err)
	}

	_, err = env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		EnvelopeXdr: envelopeBytes,
	})
	require.Equal(t, codes.Unavailable, status.Code(err))

	// wait until the rate limit resets
	time.Sleep(1 * time.Second)

	for i := 0; i < 3; i++ {
		_, err = env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
			EnvelopeXdr: envelopeBytes,
		})
		require.NoError(t, err)
	}

	_, err = env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		EnvelopeXdr: envelopeBytes,
	})
	require.Equal(t, codes.Unavailable, status.Code(err))

	env.hClient.AssertExpectations(t)
}

func TestSubmitTransaction_TextMemoNoAppID(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

	// If the text memo contains no app ID, it should still get submitted
	_, envelopeBytes, _ := generateEnvelopeWithTextMemo(t, "somerandomtext")
	hashBytes := sha256.Sum256(envelopeBytes)
	horizonResult := horizonprotocols.TransactionSuccess{
		Hash:   hex.EncodeToString(hashBytes[:]),
		Ledger: 10,
		Result: base64.StdEncoding.EncodeToString([]byte("test")),
	}
	env.hClient.On("SubmitTransaction", mock.AnythingOfType("string")).Return(horizonResult, nil).Once()

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		EnvelopeXdr: envelopeBytes,
	})

	assert.NoError(t, err)
	assert.EqualValues(t, horizonResult.Ledger, resp.Ledger)
	assert.EqualValues(t, horizonResult.Hash, hex.EncodeToString(resp.Hash.Value))
	assert.EqualValues(t, horizonResult.Result, base64.StdEncoding.EncodeToString(resp.ResultXdr))
}

func TestSubmitTransaction_TextMemoWithAppID(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

	err := env.appMapper.Add(context.Background(), "test", 1)
	require.NoError(t, err)

	err = env.appConfigStore.Add(context.Background(), 1, &app.Config{
		AppName:          "some name",
		InvoicingEnabled: true,
	})
	require.NoError(t, err)

	_, envelopeBytes, _ := generateEnvelopeWithTextMemo(t, "1-test")
	hashBytes := sha256.Sum256(envelopeBytes)
	horizonResult := horizonprotocols.TransactionSuccess{
		Hash:   hex.EncodeToString(hashBytes[:]),
		Ledger: 10,
		Result: base64.StdEncoding.EncodeToString([]byte("test")),
	}
	env.hClient.On("SubmitTransaction", mock.AnythingOfType("string")).Return(horizonResult, nil).Once()

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		EnvelopeXdr: envelopeBytes,
	})

	assert.NoError(t, err)
	assert.EqualValues(t, horizonResult.Ledger, resp.Ledger)
	assert.EqualValues(t, horizonResult.Hash, hex.EncodeToString(resp.Hash.Value))
	assert.EqualValues(t, horizonResult.Result, base64.StdEncoding.EncodeToString(resp.ResultXdr))
}

func TestSubmitTransaction_TextMemoNoMapping(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

	// If a text memo results in no mapping being found, it should still get submitted
	_, envelopeBytes, _ := generateEnvelopeWithTextMemo(t, "1-test")
	hashBytes := sha256.Sum256(envelopeBytes)
	horizonResult := horizonprotocols.TransactionSuccess{
		Hash:   hex.EncodeToString(hashBytes[:]),
		Ledger: 10,
		Result: base64.StdEncoding.EncodeToString([]byte("test")),
	}
	env.hClient.On("SubmitTransaction", mock.AnythingOfType("string")).Return(horizonResult, nil).Once()

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		EnvelopeXdr: envelopeBytes,
	})

	assert.NoError(t, err)
	assert.EqualValues(t, horizonResult.Ledger, resp.Ledger)
	assert.EqualValues(t, horizonResult.Hash, hex.EncodeToString(resp.Hash.Value))
	assert.EqualValues(t, horizonResult.Result, base64.StdEncoding.EncodeToString(resp.ResultXdr))
}

func TestSubmitTransaction_TextMemoAppNotFound(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

	// If a mapping exists but the app has no config, we treat it as an invalid app
	err := env.appMapper.Add(context.Background(), "test", 1)
	require.NoError(t, err)

	_, envelopeBytes, _ := generateEnvelopeWithTextMemo(t, "1-test")
	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		EnvelopeXdr: envelopeBytes,
		InvoiceList: il,
	})

	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Nil(t, resp)
}

func TestSubmitTransaction_TextMemoSignTransaction200WithInvoice(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

	err := env.appMapper.Add(context.Background(), "test", 1)
	require.NoError(t, err)

	_, envelopeBytes, txHash := generateEnvelopeWithTextMemo(t, "1-test")

	// Set up test server with a successful sign response
	webhookResp := &signtransaction.SuccessResponse{EnvelopeXDR: base64.StdEncoding.EncodeToString(envelopeBytes)}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)
	testServer := newTestServerWithJSONResponse(t, 200, b)

	// Set test server URL to app config
	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	err = env.appConfigStore.Add(context.Background(), 1, &app.Config{
		AppName:            "some name",
		SignTransactionURL: signURL,
		InvoicingEnabled:   true,
		WebhookSecret:      generateWebhookKey(t),
	})
	require.NoError(t, err)

	hashBytes := sha256.Sum256(envelopeBytes)
	horizonResult := horizonprotocols.TransactionSuccess{
		Hash:   hex.EncodeToString(hashBytes[:]),
		Ledger: 10,
		Result: base64.StdEncoding.EncodeToString([]byte("test")),
	}
	env.hClient.On("SubmitTransaction", mock.AnythingOfType("string")).Return(horizonResult, nil).Once()

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		EnvelopeXdr: envelopeBytes,
		InvoiceList: il,
	})
	require.NoError(t, err)

	assert.EqualValues(t, horizonResult.Ledger, resp.Ledger)
	assert.EqualValues(t, horizonResult.Hash, hex.EncodeToString(resp.Hash.Value))
	assert.EqualValues(t, horizonResult.Result, base64.StdEncoding.EncodeToString(resp.ResultXdr))

	// Ensure that the invoice got stored
	storedIL, err := env.invoiceStore.Get(context.Background(), txHash)
	require.NoError(t, err)
	assert.True(t, proto.Equal(il, storedIL))
}

func TestGetTransaction_Loader(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

	accounts := testutil.GenerateAccountIDs(t, 2)
	generated, hash := historytestutil.GenerateEntry(t, 1, 2, accounts[0], accounts[1:], nil, nil)
	require.NoError(t, env.rw.Write(context.Background(), generated))

	require.NoError(t, env.committer.Commit(context.Background(), ingestion.GetHistoryIngestorName(model.KinVersion_KIN3), nil, historytestutil.GetOrderingKey(t, generated)))

	resp, err := env.client.GetTransaction(context.Background(), &transactionpb.GetTransactionRequest{
		TransactionHash: &commonpb.TransactionHash{
			Value: hash,
		},
	})
	require.NoError(t, err)

	assert.NotNil(t, resp.Item)
	assert.Equal(t, hash, resp.Item.Hash.Value)
	assert.EqualValues(t, generated.Kind.(*model.Entry_Stellar).Stellar.ResultXdr, resp.Item.ResultXdr)
	assert.EqualValues(t, generated.Kind.(*model.Entry_Stellar).Stellar.EnvelopeXdr, resp.Item.EnvelopeXdr)
	assert.Nil(t, resp.Item.InvoiceList)
}

func TestGetTransaction_HorizonFallback(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

	_, txnEnvelopeBytes, txHash := generateEnvelope(t, nil, 0)
	horizonResult := horizonprotocols.Transaction{
		PT:          strconv.FormatInt(10<<32, 10),
		Hash:        hex.EncodeToString(txHash),
		Ledger:      10,
		ResultXdr:   base64.StdEncoding.EncodeToString([]byte("result")),
		EnvelopeXdr: base64.StdEncoding.EncodeToString(txnEnvelopeBytes),
	}

	env.hClient.On("LoadTransaction", mock.AnythingOfType("string")).Return(horizonResult, nil).Once()
	resp, err := env.client.GetTransaction(context.Background(), &transactionpb.GetTransactionRequest{
		TransactionHash: &commonpb.TransactionHash{
			Value: txHash,
		},
	})
	require.NoError(t, err)

	assert.EqualValues(t, horizonResult.Ledger, resp.Ledger)
	assert.Equal(t, transactionpb.GetTransactionResponse_SUCCESS, resp.State)
	assert.NotNil(t, resp.Item)
	assert.Equal(t, horizonResult.Hash, hex.EncodeToString(resp.Item.Hash.Value))
	assert.Equal(t, horizonResult.ResultXdr, base64.StdEncoding.EncodeToString(resp.Item.ResultXdr))
	assert.Equal(t, horizonResult.EnvelopeXdr, base64.StdEncoding.EncodeToString(resp.Item.EnvelopeXdr))
	assert.Nil(t, resp.Item.InvoiceList)
}

func TestGetTransaction_WithInvoicingEnabled(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

	signTxURL, err := url.Parse("test.kin.org/sign_tx")
	require.NoError(t, err)

	appConfig := &app.Config{
		AppName:            "kin",
		SignTransactionURL: signTxURL,
		InvoicingEnabled:   true,
	}

	err = env.appConfigStore.Add(context.Background(), 1, appConfig)
	require.NoError(t, err)

	envelope, envelopeBytes, txHash := generateEnvelope(t, il, 1)
	err = env.invoiceStore.Put(context.Background(), txHash, il)
	require.NoError(t, err)

	horizonResult := horizonprotocols.Transaction{
		Hash:        hex.EncodeToString(txHash),
		PT:          strconv.FormatInt(10<<32, 10),
		Ledger:      10,
		ResultXdr:   base64.StdEncoding.EncodeToString([]byte("result")),
		EnvelopeXdr: base64.StdEncoding.EncodeToString(envelopeBytes),
		Memo:        base64.StdEncoding.EncodeToString(marshalMemo(t, envelope.Tx.Memo)),
	}

	env.hClient.On("LoadTransaction", mock.AnythingOfType("string")).Return(horizonResult, nil).Once()
	resp, err := env.client.GetTransaction(context.Background(), &transactionpb.GetTransactionRequest{
		TransactionHash: &commonpb.TransactionHash{
			Value: txHash,
		},
	})
	require.NoError(t, err)

	assert.EqualValues(t, horizonResult.Ledger, resp.Ledger)
	assert.Equal(t, transactionpb.GetTransactionResponse_SUCCESS, resp.State)
	assert.NotNil(t, resp.Item)
	assert.Equal(t, horizonResult.Hash, hex.EncodeToString(resp.Item.Hash.Value))
	assert.Equal(t, horizonResult.ResultXdr, base64.StdEncoding.EncodeToString(resp.Item.ResultXdr))
	assert.Equal(t, horizonResult.EnvelopeXdr, base64.StdEncoding.EncodeToString(resp.Item.EnvelopeXdr))
	require.True(t, proto.Equal(il, resp.Item.InvoiceList))
}

func TestGetTransaction_WithInvoicingDisabled(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

	appConfig := &app.Config{
		AppName:          "kin",
		InvoicingEnabled: false,
	}

	err := env.appConfigStore.Add(context.Background(), 1, appConfig)
	require.NoError(t, err)

	invoiceHash, err := invoice.GetSHA224Hash(il)
	require.NoError(t, err)
	memo, err := kin.NewMemo(byte(0), kin.TransactionTypeSpend, 1, invoiceHash)
	require.NoError(t, err)

	xdrHash := xdr.Hash{}
	for i := 0; i < len(memo); i++ {
		xdrHash[i] = memo[i]
	}
	xdrMemo, err := xdr.NewMemo(xdr.MemoTypeMemoHash, xdrHash)
	require.NoError(t, err)
	binaryMemo, err := xdrMemo.MarshalBinary()
	require.NoError(t, err)

	horizonResult := horizonprotocols.Transaction{
		Hash:        hex.EncodeToString([]byte(strings.Repeat("h", 32))),
		PT:          strconv.FormatInt(10<<32, 10),
		Ledger:      10,
		ResultXdr:   base64.StdEncoding.EncodeToString([]byte("result")),
		EnvelopeXdr: base64.StdEncoding.EncodeToString([]byte("envelope")),
		Memo:        base64.StdEncoding.EncodeToString(binaryMemo),
	}

	env.hClient.On("LoadTransaction", mock.AnythingOfType("string")).Return(horizonResult, nil).Once()
	resp, err := env.client.GetTransaction(context.Background(), &transactionpb.GetTransactionRequest{
		TransactionHash: &commonpb.TransactionHash{
			Value: []byte(strings.Repeat("h", 32)),
		},
	})
	require.NoError(t, err)

	assert.EqualValues(t, horizonResult.Ledger, resp.Ledger)
	assert.Equal(t, transactionpb.GetTransactionResponse_SUCCESS, resp.State)
	assert.NotNil(t, resp.Item)
	assert.Equal(t, horizonResult.Hash, hex.EncodeToString(resp.Item.Hash.Value))
	assert.Equal(t, horizonResult.ResultXdr, base64.StdEncoding.EncodeToString(resp.Item.ResultXdr))
	assert.Equal(t, horizonResult.EnvelopeXdr, base64.StdEncoding.EncodeToString(resp.Item.EnvelopeXdr))
	assert.Nil(t, resp.Item.InvoiceList)
}

func TestGetTransaction_HorizonErrors(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

	type testCase struct {
		hError   horizon.Error
		resp     *transactionpb.GetTransactionResponse
		grpcCode codes.Code
	}

	testCases := []testCase{
		{
			hError: horizon.Error{
				Problem: horizon.Problem{
					Status: 404,
				},
			},
			resp:     &transactionpb.GetTransactionResponse{State: transactionpb.GetTransactionResponse_UNKNOWN},
			grpcCode: codes.OK,
		},
		{
			hError: horizon.Error{
				Problem: horizon.Problem{
					Status: 500,
				},
			},
			resp:     nil,
			grpcCode: codes.Internal,
		},
	}

	for _, tc := range testCases {
		env.hClient.On("LoadTransaction", mock.AnythingOfType("string")).Return(horizonprotocols.Transaction{}, error(&tc.hError)).Once()
		resp, err := env.client.GetTransaction(context.Background(), &transactionpb.GetTransactionRequest{
			TransactionHash: &commonpb.TransactionHash{
				Value: make([]byte, 32),
			},
		})
		assert.Equal(t, tc.grpcCode, status.Code(err))
		assert.True(t, proto.Equal(tc.resp, resp))
	}
}

func TestGetTransaction_TextMemo(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

	err := env.appMapper.Add(context.Background(), "test", 1)
	require.NoError(t, err)

	signTxURL, err := url.Parse("test.kin.org/sign_tx")
	require.NoError(t, err)

	appConfig := &app.Config{
		AppName:            "kin",
		SignTransactionURL: signTxURL,
		InvoicingEnabled:   true,
	}

	err = env.appConfigStore.Add(context.Background(), 1, appConfig)
	require.NoError(t, err)

	envelope, envelopeBytes, txHash := generateEnvelopeWithTextMemo(t, "1-test")
	err = env.invoiceStore.Put(context.Background(), txHash, il)
	require.NoError(t, err)

	horizonResult := horizonprotocols.Transaction{
		Hash:        hex.EncodeToString(txHash),
		PT:          strconv.FormatInt(10<<32, 10),
		Ledger:      10,
		ResultXdr:   base64.StdEncoding.EncodeToString([]byte("result")),
		EnvelopeXdr: base64.StdEncoding.EncodeToString(envelopeBytes),
		Memo:        base64.StdEncoding.EncodeToString(marshalMemo(t, envelope.Tx.Memo)),
	}

	env.hClient.On("LoadTransaction", mock.AnythingOfType("string")).Return(horizonResult, nil).Once()
	resp, err := env.client.GetTransaction(context.Background(), &transactionpb.GetTransactionRequest{
		TransactionHash: &commonpb.TransactionHash{
			Value: txHash,
		},
	})
	require.NoError(t, err)

	assert.EqualValues(t, horizonResult.Ledger, resp.Ledger)
	assert.Equal(t, transactionpb.GetTransactionResponse_SUCCESS, resp.State)
	assert.NotNil(t, resp.Item)
	assert.Equal(t, horizonResult.Hash, hex.EncodeToString(resp.Item.Hash.Value))
	assert.Equal(t, horizonResult.ResultXdr, base64.StdEncoding.EncodeToString(resp.Item.ResultXdr))
	assert.Equal(t, horizonResult.EnvelopeXdr, base64.StdEncoding.EncodeToString(resp.Item.EnvelopeXdr))
	require.True(t, proto.Equal(il, resp.Item.InvoiceList))
}

func TestGetHistory_Query(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

	accounts := testutil.GenerateAccountIDs(t, 10)
	generated := make([]*model.Entry, 20)
	for i := 0; i < len(generated); i++ {
		generated[i], _ = historytestutil.GenerateEntry(t, uint64(i-i%2), i, accounts[0], accounts[1:], nil, nil)
		require.NoError(t, env.rw.Write(context.Background(), generated[i]))
	}

	// Request history from beginning, but without any committed entries.
	resp, err := env.client.GetHistory(context.Background(), &transactionpb.GetHistoryRequest{
		AccountId: &commonpb.StellarAccountId{
			Value: accounts[0].Address(),
		},
	})
	require.NoError(t, err)
	require.Empty(t, resp.Items)

	// Advance to the 5th entry
	require.NoError(t, env.committer.Commit(context.Background(), ingestion.GetHistoryIngestorName(model.KinVersion_KIN3), nil, historytestutil.GetOrderingKey(t, generated[4])))
	resp, err = env.client.GetHistory(context.Background(), &transactionpb.GetHistoryRequest{
		AccountId: &commonpb.StellarAccountId{
			Value: accounts[0].Address(),
		},
	})
	require.NoError(t, err)
	require.Len(t, resp.Items, 5)
	for i, item := range resp.Items {
		expected, err := txDataFromEntry(generated[i])
		require.NoError(t, err)

		assert.EqualValues(t, expected.hash, item.Hash.Value)
		assert.EqualValues(t, expected.resultXDR, item.ResultXdr)
		assert.EqualValues(t, expected.envelopeXDR, item.EnvelopeXdr)
	}

	// Mark all as committed
	previous := historytestutil.GetOrderingKey(t, generated[4])
	latest := historytestutil.GetOrderingKey(t, generated[len(generated)-1])
	require.NoError(t, env.committer.Commit(context.Background(), ingestion.GetHistoryIngestorName(model.KinVersion_KIN3), previous, latest))

	testCases := []struct {
		direction transactionpb.GetHistoryRequest_Direction
		start     int
		expected  []*model.Entry
	}{
		{
			start:    -1,
			expected: generated,
		},
		{
			start:    10,
			expected: generated[10:],
		},
		{
			start:     -1,
			direction: transactionpb.GetHistoryRequest_DESC,
			// Since we haven't specified a cursor, the loader should
			// default to the _latest_ entry as a cursor.
			expected: generated,
		},
		{
			start:     0,
			direction: transactionpb.GetHistoryRequest_DESC,
			expected:  generated[0:1],
		},
		{
			direction: transactionpb.GetHistoryRequest_DESC,
			start:     len(generated) - 1,
			expected:  generated,
		},
	}

	for _, tc := range testCases {
		var cursor *transactionpb.Cursor
		if tc.start >= 0 {
			k, err := generated[tc.start].GetOrderingKey()
			require.NoError(t, err)
			cursor = &transactionpb.Cursor{
				Value: k,
			}
		}

		resp, err := env.client.GetHistory(context.Background(), &transactionpb.GetHistoryRequest{
			AccountId: &commonpb.StellarAccountId{
				Value: accounts[0].Address(),
			},
			Direction: tc.direction,
			Cursor:    cursor,
		})
		require.NoError(t, err)
		require.Equal(t, len(tc.expected), len(resp.Items))

		for i := 0; i < len(tc.expected); i++ {
			var expected txData
			if tc.direction == transactionpb.GetHistoryRequest_ASC {
				expected, err = txDataFromEntry(tc.expected[i])
			} else {
				expected, err = txDataFromEntry(tc.expected[len(tc.expected)-1-i])
			}
			require.NoError(t, err)

			assert.EqualValues(t, expected.hash, resp.Items[i].Hash.Value)
			assert.EqualValues(t, expected.resultXDR, resp.Items[i].ResultXdr)
			assert.EqualValues(t, expected.envelopeXDR, resp.Items[i].EnvelopeXdr)
		}
	}
}

func TestGetHistory_WithInvoicingEnabled(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

	signTxURL, err := url.Parse("test.kin.org/sign_tx")
	require.NoError(t, err)

	appConfig := &app.Config{
		AppName:            "kin",
		SignTransactionURL: signTxURL,
		InvoicingEnabled:   true,
	}

	err = env.appConfigStore.Add(context.Background(), 1, appConfig)
	require.NoError(t, err)

	ilBytes, err := proto.Marshal(il)
	require.NoError(t, err)
	ilHash := sha256.Sum224(ilBytes)

	accounts := testutil.GenerateAccountIDs(t, 21)
	generated := make([]*model.Entry, 20)
	hashes := make([][]byte, 20)
	for i := 0; i < len(generated); i++ {
		generated[i], hashes[i] = historytestutil.GenerateEntry(t, uint64(i-i%2), i, accounts[0], accounts[i:i+1], ilHash[:], nil)
		require.NoError(t, env.rw.Write(context.Background(), generated[i]))
	}

	require.NoError(t, env.committer.Commit(context.Background(), ingestion.GetHistoryIngestorName(model.KinVersion_KIN3), nil, historytestutil.GetOrderingKey(t, generated[len(generated)-1])))

	for _, hash := range hashes {
		require.NoError(t, env.invoiceStore.Put(context.Background(), hash, il))
	}

	resp, err := env.client.GetHistory(context.Background(), &transactionpb.GetHistoryRequest{
		AccountId: &commonpb.StellarAccountId{
			Value: accounts[0].Address(),
		},
	})
	require.NoError(t, err)
	require.Len(t, resp.Items, len(generated))

	for _, item := range resp.Items {
		require.NotNil(t, item.InvoiceList)
		require.True(t, proto.Equal(il, item.InvoiceList))
	}
}

func TestGetHistory_WithInvoicingDisabled(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

	signTxURL, err := url.Parse("test.kin.org/sign_tx")
	require.NoError(t, err)

	appConfig := &app.Config{
		AppName:            "kin",
		SignTransactionURL: signTxURL,
		InvoicingEnabled:   false,
	}

	err = env.appConfigStore.Add(context.Background(), 1, appConfig)
	require.NoError(t, err)

	ilBytes, err := proto.Marshal(il)
	require.NoError(t, err)
	ilHash := sha256.Sum224(ilBytes)

	accounts := testutil.GenerateAccountIDs(t, 21)
	generated := make([]*model.Entry, 20)
	hashes := make([][]byte, 20)
	for i := 0; i < len(generated); i++ {
		generated[i], hashes[i] = historytestutil.GenerateEntry(t, uint64(i-i%2), i, accounts[0], accounts[i:i+1], ilHash[:], nil)
		require.NoError(t, env.rw.Write(context.Background(), generated[i]))
	}

	require.NoError(t, env.committer.Commit(context.Background(), ingestion.GetHistoryIngestorName(model.KinVersion_KIN3), nil, historytestutil.GetOrderingKey(t, generated[len(generated)-1])))

	for _, hash := range hashes {
		require.NoError(t, env.invoiceStore.Put(context.Background(), hash, il))
	}

	resp, err := env.client.GetHistory(context.Background(), &transactionpb.GetHistoryRequest{
		AccountId: &commonpb.StellarAccountId{
			Value: accounts[0].Address(),
		},
	})
	require.NoError(t, err)
	require.Len(t, resp.Items, len(generated))

	for _, item := range resp.Items {
		require.Nil(t, item.InvoiceList)
	}
}

func TestGetHistory_TextMemoWithInvoicingEnabled(t *testing.T) {
	env, cleanup := setup(t, -1, -1)
	defer cleanup()

	err := env.appMapper.Add(context.Background(), "test", 1)
	require.NoError(t, err)

	signTxURL, err := url.Parse("test.kin.org/sign_tx")
	require.NoError(t, err)

	appConfig := &app.Config{
		AppName:            "kin",
		SignTransactionURL: signTxURL,
		InvoicingEnabled:   true,
	}

	err = env.appConfigStore.Add(context.Background(), 1, appConfig)
	require.NoError(t, err)

	accounts := testutil.GenerateAccountIDs(t, 21)
	generated := make([]*model.Entry, 20)
	hashes := make([][]byte, 20)
	textMemo := "1-test"
	for i := 0; i < len(generated); i++ {
		generated[i], hashes[i] = historytestutil.GenerateEntry(t, uint64(i-i%2), i, accounts[0], accounts[i:i+1], nil, &textMemo)
		require.NoError(t, env.rw.Write(context.Background(), generated[i]))
	}

	require.NoError(t, env.committer.Commit(context.Background(), ingestion.GetHistoryIngestorName(model.KinVersion_KIN3), nil, historytestutil.GetOrderingKey(t, generated[len(generated)-1])))

	for _, hash := range hashes {
		require.NoError(t, env.invoiceStore.Put(context.Background(), hash, il))
	}

	resp, err := env.client.GetHistory(context.Background(), &transactionpb.GetHistoryRequest{
		AccountId: &commonpb.StellarAccountId{
			Value: accounts[0].Address(),
		},
	})
	require.NoError(t, err)
	require.Len(t, resp.Items, len(generated))

	for _, item := range resp.Items {
		require.NotNil(t, item.InvoiceList)
		require.True(t, proto.Equal(il, item.InvoiceList))
	}
}

// todo: options may be better for longer term testing
func generateEnvelope(t *testing.T, invoiceList *commonpb.InvoiceList, appIndex uint16) (envelope xdr.TransactionEnvelope, envelopeBytes, txHash []byte) {
	sender, err := keypair.Random()
	require.NoError(t, err)

	pubKey, err := strkey.Decode(strkey.VersionByteAccountID, sender.Address())
	require.NoError(t, err)
	var senderPubKey xdr.Uint256
	copy(senderPubKey[:], pubKey)

	senderAcc := xdr.AccountId{
		Type:    xdr.PublicKeyTypePublicKeyTypeEd25519,
		Ed25519: &senderPubKey,
	}
	txnEnvelope := xdr.TransactionEnvelope{
		Tx: xdr.Transaction{
			SourceAccount: senderAcc,
			Operations: []xdr.Operation{
				{
					Body: xdr.OperationBody{
						Type: xdr.OperationTypePayment,
						PaymentOp: &xdr.PaymentOp{
							Destination: senderAcc,
						},
					},
				},
			},
		},
	}

	if invoiceList != nil {
		ilHash, err := invoice.GetSHA224Hash(invoiceList)
		require.NoError(t, err)

		memo, err := kin.NewMemo(byte(0), kin.TransactionTypeSpend, appIndex, ilHash)
		require.NoError(t, err)
		xdrHash := xdr.Hash{}
		for i := 0; i < len(memo); i++ {
			xdrHash[i] = memo[i]
		}

		xdrMemo, err := xdr.NewMemo(xdr.MemoTypeMemoHash, xdrHash)
		require.NoError(t, err)

		txnEnvelope.Tx.Memo = xdrMemo
		txnEnvelope.Tx.Operations = make([]xdr.Operation, len(invoiceList.Invoices))
		for i := range invoiceList.Invoices {
			txnEnvelope.Tx.Operations[i] = xdr.Operation{
				Body: xdr.OperationBody{
					Type: xdr.OperationTypePayment,
					PaymentOp: &xdr.PaymentOp{
						Destination: senderAcc,
					},
				},
			}
		}
	}

	n, err := kin.GetNetwork()
	require.NoError(t, err)
	signedEnvelope, err := transaction.SignEnvelope(&txnEnvelope, n, sender.Seed())
	require.NoError(t, err)

	hash, err := network.HashTransaction(&txnEnvelope.Tx, n.Passphrase)
	require.NoError(t, err)

	envelopeBytes, err = signedEnvelope.MarshalBinary()
	require.NoError(t, err)

	return txnEnvelope, envelopeBytes, hash[:]
}

func generateEnvelopeWithTextMemo(t *testing.T, textMemo string) (envelope xdr.TransactionEnvelope, envelopeBytes, txHash []byte) {
	sender, err := keypair.Random()
	require.NoError(t, err)

	pubKey, err := strkey.Decode(strkey.VersionByteAccountID, sender.Address())
	require.NoError(t, err)
	var senderPubKey xdr.Uint256
	copy(senderPubKey[:], pubKey)

	senderAcc := xdr.AccountId{
		Type:    xdr.PublicKeyTypePublicKeyTypeEd25519,
		Ed25519: &senderPubKey,
	}
	txnEnvelope := xdr.TransactionEnvelope{
		Tx: xdr.Transaction{
			SourceAccount: senderAcc,
			Operations: []xdr.Operation{
				{
					Body: xdr.OperationBody{
						Type: xdr.OperationTypePayment,
						PaymentOp: &xdr.PaymentOp{
							Destination: senderAcc,
						},
					},
				},
			},
		},
	}

	xdrMemo, err := xdr.NewMemo(xdr.MemoTypeMemoText, textMemo)
	require.NoError(t, err)

	txnEnvelope.Tx.Memo = xdrMemo

	n, err := kin.GetNetwork()
	require.NoError(t, err)
	signedEnvelope, err := transaction.SignEnvelope(&txnEnvelope, n, sender.Seed())
	require.NoError(t, err)

	hash, err := network.HashTransaction(&txnEnvelope.Tx, n.Passphrase)
	require.NoError(t, err)

	envelopeBytes, err = signedEnvelope.MarshalBinary()
	require.NoError(t, err)

	return txnEnvelope, envelopeBytes, hash[:]
}

func marshalMemo(t *testing.T, memo xdr.Memo) []byte {
	b, err := memo.MarshalBinary()
	require.NoError(t, err)
	return b
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
