package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"
	agoraenv "github.com/kinecosystem/agora-common/env"
	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/agora-common/testutil"
	"github.com/kinecosystem/go/clients/horizon"
	"github.com/kinecosystem/go/keypair"
	horizonprotocols "github.com/kinecosystem/go/protocols/horizon"
	"github.com/kinecosystem/go/strkey"
	"github.com/kinecosystem/go/xdr"
	"github.com/stellar/go/clients/horizonclient"
	horizonprotocolsv2 "github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/support/render/problem"
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
	"github.com/kinecosystem/agora/pkg/invoice"
	invoicedb "github.com/kinecosystem/agora/pkg/invoice/memory"
	"github.com/kinecosystem/agora/pkg/transaction"
	"github.com/kinecosystem/agora/pkg/webhook"
	"github.com/kinecosystem/agora/pkg/webhook/signtransaction"
)

var (
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
	client           transactionpb.TransactionClient
	whitelistAccount *keypair.Full

	hClient   *horizon.MockClient
	hClientV2 *horizonclient.MockClient

	appConfigStore app.ConfigStore
	invoiceStore   invoice.Store
}

func setup(t *testing.T, enableWhitelist bool) (env testEnv, cleanup func()) {
	os.Setenv("AGORA_ENVIRONMENT", string(agoraenv.AgoraEnvironmentDev))

	if enableWhitelist {
		var err error
		env.whitelistAccount, err = keypair.Random()
		require.NoError(t, err)
	}

	conn, serv, err := testutil.NewServer()
	require.NoError(t, err)

	env.client = transactionpb.NewTransactionClient(conn)
	env.hClient = &horizon.MockClient{}
	env.hClientV2 = &horizonclient.MockClient{}

	env.appConfigStore = appconfigdb.New()
	env.invoiceStore = invoicedb.New()
	s, err := New(env.whitelistAccount, env.appConfigStore, env.invoiceStore, env.hClient, env.hClientV2, webhook.NewClient(http.DefaultClient))
	require.NoError(t, err)
	serv.RegisterService(func(server *grpc.Server) {
		transactionpb.RegisterTransactionServer(server, s)
	})

	cleanup, err = serv.Serve()
	require.NoError(t, err)

	return env, cleanup
}

func TestSubmit_NoKinMemo(t *testing.T) {
	env, cleanup := setup(t, false)
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

func TestSubmitTransaction_TestAppWhitelist_Enabled(t *testing.T) {
	env, cleanup := setup(t, true)
	defer cleanup()

	_, envelopeBytes, txHash := generateEnvelope(t, il, 0)
	horizonResult := horizonprotocols.TransactionSuccess{
		Hash:   hex.EncodeToString(txHash),
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
	require.Len(t, env.hClient.Calls, 1)

	var submittedEnvelope xdr.TransactionEnvelope
	submittedEnvelopeBytes, err := base64.StdEncoding.DecodeString(env.hClient.Calls[0].Arguments[0].(string))
	require.NoError(t, err)
	require.NoError(t, submittedEnvelope.UnmarshalBinary(submittedEnvelopeBytes))
	require.Len(t, submittedEnvelope.Signatures, 2)
	assert.False(t, bytes.Equal(submittedEnvelope.Signatures[0].Signature, submittedEnvelope.Signatures[1].Signature))

	network, err := kin.GetNetwork()
	require.NoError(t, err)

	// If we sign it with the whitelist account, the signature should match what
	// the serverice signed with
	signed, err := transaction.SignEnvelope(&submittedEnvelope, network, env.whitelistAccount.Seed())
	require.NoError(t, err)
	assert.Equal(t, signed.Signatures[1], signed.Signatures[2])
}

func TestSubmitTransaction_NonTestAppWhitelist_Enabled(t *testing.T) {
	env, cleanup := setup(t, true)
	defer cleanup()

	err := env.appConfigStore.Add(context.Background(), 1, &app.Config{
		AppName:          "some name",
		InvoicingEnabled: true,
	})
	require.NoError(t, err)

	_, envelopeBytes, txHash := generateEnvelope(t, il, 1)
	horizonResult := horizonprotocols.TransactionSuccess{
		Hash:   hex.EncodeToString(txHash),
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
	require.Len(t, env.hClient.Calls, 1)

	var submittedEnvelope xdr.TransactionEnvelope
	submittedEnvelopeBytes, err := base64.StdEncoding.DecodeString(env.hClient.Calls[0].Arguments[0].(string))
	require.NoError(t, err)
	require.NoError(t, submittedEnvelope.UnmarshalBinary(submittedEnvelopeBytes))
	require.Len(t, submittedEnvelope.Signatures, 1)
}

func TestSubmitTransaction_TestAppWhitelist_Disabled(t *testing.T) {
	os.Setenv("AGORA_ENVIRONMENT", string(agoraenv.AgoraEnvironmentProd))
	env, cleanup := setup(t, false)
	defer cleanup()

	err := env.appConfigStore.Add(context.Background(), 0, &app.Config{
		AppName:          "some name",
		InvoicingEnabled: true,
	})
	require.NoError(t, err)

	_, envelopeBytes, txHash := generateEnvelope(t, il, 0)
	horizonResult := horizonprotocols.TransactionSuccess{
		Hash:   hex.EncodeToString(txHash),
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
	require.Len(t, env.hClient.Calls, 1)

	var submittedEnvelope xdr.TransactionEnvelope
	submittedEnvelopeBytes, err := base64.StdEncoding.DecodeString(env.hClient.Calls[0].Arguments[0].(string))
	require.NoError(t, err)
	require.NoError(t, submittedEnvelope.UnmarshalBinary(submittedEnvelopeBytes))
	require.Len(t, submittedEnvelope.Signatures, 1)
}

func TestSubmitTransaction_AppNotFound(t *testing.T) {
	env, cleanup := setup(t, false)
	defer cleanup()

	_, envelopeBytes, _ := generateEnvelope(t, il, 0)
	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		EnvelopeXdr: envelopeBytes,
		InvoiceList: il,
	})

	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Nil(t, resp)
}

func TestSubmitTransaction_AppSignTxURLNotSet(t *testing.T) {
	env, cleanup := setup(t, false)
	defer cleanup()

	err := env.appConfigStore.Add(context.Background(), 0, &app.Config{
		AppName:          "some name",
		InvoicingEnabled: true,
	})
	require.NoError(t, err)

	_, envelopeBytes, _ := generateEnvelope(t, il, 0)
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
	env, cleanup := setup(t, false)
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
	})
	require.NoError(t, err)

	_, envelopeBytes, _ := generateEnvelope(t, il, 1)
	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		EnvelopeXdr: envelopeBytes,
	})

	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Nil(t, resp)
}

func TestSubmitTransaction_SignTransaction403(t *testing.T) {
	env, cleanup := setup(t, false)
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
	env, cleanup := setup(t, false)
	defer cleanup()

	_, envelopeBytes, _ := generateEnvelope(t, il, 1)

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

	assert.NoError(t, err)
	assert.EqualValues(t, horizonResult.Ledger, resp.Ledger)
	assert.EqualValues(t, horizonResult.Hash, hex.EncodeToString(resp.Hash.Value))
	assert.EqualValues(t, horizonResult.Result, base64.StdEncoding.EncodeToString(resp.ResultXdr))
}

func TestSubmitTransaction_SignTransaction200InvalidResponse(t *testing.T) {
	env, cleanup := setup(t, false)
	defer cleanup()

	_, envelopeBytes, _ := generateEnvelope(t, il, 1)

	// Set up test server with a successful sign response
	webhookResp := &signtransaction.SuccessResponse{EnvelopeXDR: "invalidxdr"}
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
	})
	require.NoError(t, err)

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		EnvelopeXdr: envelopeBytes,
	})

	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Nil(t, resp)
}

func TestSubmitTransaction_InvalidInvoiceList(t *testing.T) {
	env, cleanup := setup(t, false)
	defer cleanup()

	// mismatch counts
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
			{
				Items: []*commonpb.Invoice_LineItem{
					{
						Title:  "lineitem2",
						Amount: 1,
					},
				},
			},
		},
	}

	_, envelopeBytes, _ := generateEnvelope(t, invalid, 0)
	_, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		EnvelopeXdr: envelopeBytes,
		InvoiceList: invalid,
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	for i := int64(0); i < 100; i++ {
		invalid.Invoices = append(invalid.Invoices, &commonpb.Invoice{
			Items: []*commonpb.Invoice_LineItem{
				{
					Title:  fmt.Sprintf("lineitem%d", 3+i),
					Amount: i,
				},
			},
		})
	}

	_, envelopeBytes, _ = generateEnvelope(t, invalid, 0)
	_, err = env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		EnvelopeXdr: envelopeBytes,
		InvoiceList: invalid,
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestSubmitTransaction_WithInvoiceInvalidMemo(t *testing.T) {
	env, cleanup := setup(t, false)
	defer cleanup()

	envelope, _, _ := generateEnvelope(t, nil, 0)

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
	env, cleanup := setup(t, false)
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

func TestSubmit_HorizonErrors(t *testing.T) {
	env, cleanup := setup(t, false)
	defer cleanup()

	_, envelopeBytes, _ := generateEnvelope(t, nil, 0)

	type testCase struct {
		hError   horizon.Error
		grpcCode codes.Code
	}

	testCases := []testCase{
		{
			hError: horizon.Error{
				Problem: horizon.Problem{
					Status: 500,
				},
			},
			grpcCode: codes.Internal,
		},
	}

	for _, tc := range testCases {
		env.hClient.On("SubmitTransaction", mock.AnythingOfType("string")).Return(horizonprotocols.TransactionSuccess{}, error(&tc.hError)).Once()
		_, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
			EnvelopeXdr: envelopeBytes,
		})
		assert.Equal(t, tc.grpcCode, status.Code(err))
	}
}

func TestGetTransaction_Happy(t *testing.T) {
	env, cleanup := setup(t, false)
	defer cleanup()

	_, txnEnvelopeBytes, txHash := generateEnvelope(t, nil, 0)
	horizonResult := horizonprotocols.Transaction{
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
	env, cleanup := setup(t, false)
	defer cleanup()

	signTxURL, err := url.Parse("test.kin.org/sign_tx")
	require.NoError(t, err)

	appConfig := &app.Config{
		AppName:            "kin",
		SignTransactionURL: signTxURL,
		InvoicingEnabled:   true,
	}

	err = env.appConfigStore.Add(context.Background(), 0, appConfig)
	require.NoError(t, err)

	envelope, envelopeBytes, txHash := generateEnvelope(t, il, 0)
	err = env.invoiceStore.Put(context.Background(), txHash, il)
	require.NoError(t, err)

	horizonResult := horizonprotocols.Transaction{
		Hash:        hex.EncodeToString(txHash),
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
	env, cleanup := setup(t, false)
	defer cleanup()

	appConfig := &app.Config{
		AppName:          "kin",
		InvoicingEnabled: false,
	}

	err := env.appConfigStore.Add(context.Background(), 0, appConfig)
	require.NoError(t, err)

	invoiceHash, err := invoice.GetSHA224Hash(il)
	require.NoError(t, err)
	memo, err := kin.NewMemo(byte(0), kin.TransactionTypeSpend, 0, invoiceHash)
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
	env, cleanup := setup(t, false)
	defer cleanup()

	type testCase struct {
		hError   horizon.Error
		grpcCode codes.Code
	}

	testCases := []testCase{
		{
			hError: horizon.Error{
				Problem: horizon.Problem{
					Status: 404,
				},
			},
			grpcCode: codes.NotFound,
		},
		{
			hError: horizon.Error{
				Problem: horizon.Problem{
					Status: 500,
				},
			},
			grpcCode: codes.Internal,
		},
	}

	for _, tc := range testCases {
		env.hClient.On("LoadTransaction", mock.AnythingOfType("string")).Return(horizonprotocols.Transaction{}, error(&tc.hError)).Once()
		_, err := env.client.GetTransaction(context.Background(), &transactionpb.GetTransactionRequest{
			TransactionHash: &commonpb.TransactionHash{
				Value: make([]byte, 32),
			},
		})
		assert.Equal(t, tc.grpcCode, status.Code(err))
	}
}

func TestGetHistory_Happy(t *testing.T) {
	env, cleanup := setup(t, false)
	defer cleanup()

	page := horizonprotocolsv2.TransactionsPage{
		Embedded: struct {
			Records []horizonprotocolsv2.Transaction
		}{
			Records: []horizonprotocolsv2.Transaction{
				{
					PT:          "cursor",
					Hash:        hex.EncodeToString([]byte(strings.Repeat("h", 32))),
					ResultXdr:   base64.StdEncoding.EncodeToString([]byte("resultXdr")),
					EnvelopeXdr: base64.StdEncoding.EncodeToString([]byte("envelopeXdr")),
					Successful:  true,
					Ledger:      1,
				},
			},
		},
	}

	type testCase struct {
		cursor    string
		direction horizonclient.Order
		request   *transactionpb.GetHistoryRequest
	}

	testCases := []testCase{
		{
			// No cursor or direction specified
			request:   &transactionpb.GetHistoryRequest{},
			direction: horizonclient.OrderAsc,
		},
		{
			request: &transactionpb.GetHistoryRequest{
				Cursor: &transactionpb.Cursor{
					Value: []byte("abc"),
				},
			},
			cursor:    "abc",
			direction: horizonclient.OrderAsc,
		},
		{
			request: &transactionpb.GetHistoryRequest{
				Direction: transactionpb.GetHistoryRequest_DESC,
			},
			direction: horizonclient.OrderDesc,
		},
		{
			request: &transactionpb.GetHistoryRequest{
				Cursor: &transactionpb.Cursor{
					Value: []byte("def"),
				},
				Direction: transactionpb.GetHistoryRequest_DESC,
			},
			cursor:    "def",
			direction: horizonclient.OrderDesc,
		},
	}

	for i, tc := range testCases {
		env.hClientV2.On("Transactions", mock.Anything).Return(page, nil).Once()

		tc.request.AccountId = &commonpb.StellarAccountId{
			Value: strings.Repeat("G", 56),
		}

		resp, err := env.client.GetHistory(context.Background(), tc.request)
		require.NoError(t, err)

		require.Len(t, resp.Items, 1)
		item := resp.Items[0]
		assert.Equal(t, page.Embedded.Records[0].Hash, hex.EncodeToString(item.Hash.Value))
		assert.Equal(t, page.Embedded.Records[0].ResultXdr, base64.StdEncoding.EncodeToString(item.ResultXdr))
		assert.Equal(t, page.Embedded.Records[0].EnvelopeXdr, base64.StdEncoding.EncodeToString(item.EnvelopeXdr))
		assert.Nil(t, item.InvoiceList)

		require.Len(t, env.hClientV2.Calls, i+1)
		txnReq := env.hClientV2.Calls[i].Arguments[0].(horizonclient.TransactionRequest)
		assert.Equal(t, tc.cursor, txnReq.Cursor)
		assert.Equal(t, tc.direction, txnReq.Order)
	}
}

func TestGetHistory_WithInvoicingEnabled(t *testing.T) {
	env, cleanup := setup(t, false)
	defer cleanup()

	signTxURL, err := url.Parse("test.kin.org/sign_tx")
	require.NoError(t, err)

	appConfig := &app.Config{
		AppName:            "kin",
		SignTransactionURL: signTxURL,
		InvoicingEnabled:   true,
	}

	err = env.appConfigStore.Add(context.Background(), 0, appConfig)
	require.NoError(t, err)

	envelope, envelopeBytes, txHash := generateEnvelope(t, il, 0)
	err = env.invoiceStore.Put(context.Background(), txHash, il)
	require.NoError(t, err)

	page := horizonprotocolsv2.TransactionsPage{
		Embedded: struct {
			Records []horizonprotocolsv2.Transaction
		}{
			Records: []horizonprotocolsv2.Transaction{
				{
					PT:          "cursor",
					Hash:        hex.EncodeToString(txHash),
					ResultXdr:   base64.StdEncoding.EncodeToString([]byte("resultXdr")),
					EnvelopeXdr: base64.StdEncoding.EncodeToString(envelopeBytes),
					Successful:  true,
					Ledger:      1,
					Memo:        base64.StdEncoding.EncodeToString(marshalMemo(t, envelope.Tx.Memo)),
				},
			},
		},
	}
	env.hClientV2.On("Transactions", mock.Anything).Return(page, nil).Once()

	resp, err := env.client.GetHistory(context.Background(), &transactionpb.GetHistoryRequest{
		AccountId: &commonpb.StellarAccountId{
			Value: strings.Repeat("G", 56),
		},
	})
	require.NoError(t, err)

	require.Len(t, resp.Items, 1)
	require.NotNil(t, resp.Items[0].InvoiceList)
	item := resp.Items[0]
	assert.Equal(t, page.Embedded.Records[0].Hash, hex.EncodeToString(item.Hash.Value))
	assert.Equal(t, page.Embedded.Records[0].ResultXdr, base64.StdEncoding.EncodeToString(item.ResultXdr))
	assert.Equal(t, page.Embedded.Records[0].EnvelopeXdr, base64.StdEncoding.EncodeToString(item.EnvelopeXdr))

	// TODO: assert all agora data fields when fully implemented
	require.True(t, proto.Equal(il, item.InvoiceList))

	require.Len(t, env.hClientV2.Calls, 1)
	txnReq := env.hClientV2.Calls[0].Arguments[0].(horizonclient.TransactionRequest)
	assert.Equal(t, "", txnReq.Cursor)
	assert.Equal(t, horizonclient.OrderAsc, txnReq.Order)
}

func TestGetHistory_WithInvoicingDisabled(t *testing.T) {
	env, cleanup := setup(t, false)
	defer cleanup()

	appConfig := &app.Config{
		AppName:          "kin",
		InvoicingEnabled: false,
	}

	err := env.appConfigStore.Add(context.Background(), 0, appConfig)
	require.NoError(t, err)

	envelope, envelopeBytes, txHash := generateEnvelope(t, il, 0)
	page := horizonprotocolsv2.TransactionsPage{
		Embedded: struct {
			Records []horizonprotocolsv2.Transaction
		}{
			Records: []horizonprotocolsv2.Transaction{
				{
					PT:          "cursor",
					Hash:        hex.EncodeToString(txHash),
					ResultXdr:   base64.StdEncoding.EncodeToString([]byte("resultXdr")),
					EnvelopeXdr: base64.StdEncoding.EncodeToString(envelopeBytes),
					Successful:  true,
					Ledger:      1,
					Memo:        base64.StdEncoding.EncodeToString(marshalMemo(t, envelope.Tx.Memo)),
				},
			},
		},
	}
	env.hClientV2.On("Transactions", mock.Anything).Return(page, nil).Once()

	resp, err := env.client.GetHistory(context.Background(), &transactionpb.GetHistoryRequest{
		AccountId: &commonpb.StellarAccountId{
			Value: strings.Repeat("G", 56),
		},
	})
	require.NoError(t, err)

	require.Len(t, resp.Items, 1)
	item := resp.Items[0]
	assert.Equal(t, page.Embedded.Records[0].Hash, hex.EncodeToString(item.Hash.Value))
	assert.Equal(t, page.Embedded.Records[0].ResultXdr, base64.StdEncoding.EncodeToString(item.ResultXdr))
	assert.Equal(t, page.Embedded.Records[0].EnvelopeXdr, base64.StdEncoding.EncodeToString(item.EnvelopeXdr))
	assert.Nil(t, item.InvoiceList)
}

func TestGetHistory_HorizonErrors(t *testing.T) {
	env, cleanup := setup(t, false)
	defer cleanup()

	type testCase struct {
		hError   horizonclient.Error
		grpcCode codes.Code
	}

	testCases := []testCase{
		{
			hError: horizonclient.Error{
				Problem: problem.P{
					Status: 404,
				},
			},
			grpcCode: codes.NotFound,
		},
		{
			hError: horizonclient.Error{
				Problem: problem.P{
					Status: 500,
				},
			},
			grpcCode: codes.Internal,
		},
	}

	for _, tc := range testCases {
		env.hClientV2.On("Transactions", mock.Anything).Return(horizonprotocolsv2.TransactionsPage{}, error(&tc.hError)).Once()
		_, err := env.client.GetHistory(context.Background(), &transactionpb.GetHistoryRequest{
			AccountId: &commonpb.StellarAccountId{
				Value: strings.Repeat("G", 56),
			},
		})
		assert.Equal(t, tc.grpcCode, status.Code(err))
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

	txBytes, err := txnEnvelope.Tx.MarshalBinary()
	require.NoError(t, err)
	hash := sha256.Sum256(txBytes)

	network, err := kin.GetNetwork()
	require.NoError(t, err)
	signedEnvelope, err := transaction.SignEnvelope(&txnEnvelope, network, sender.Seed())
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
