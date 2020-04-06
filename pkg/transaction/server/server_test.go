package server

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/kinecosystem/agora-common/testutil"
	"github.com/kinecosystem/go/clients/horizon"
	horizonprotocols "github.com/kinecosystem/go/protocols/horizon"
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

	commonpb "github.com/kinecosystem/kin-api/genproto/common/v3"
	transactionpb "github.com/kinecosystem/kin-api/genproto/transaction/v3"

	"github.com/kinecosystem/agora-transaction-services-internal/pkg/appindex/static"
	"github.com/kinecosystem/agora-transaction-services-internal/pkg/data/memory"
)

var (
	emptyAcc xdr.Uint256
	emptyTxn = xdr.Transaction{
		SourceAccount: xdr.AccountId{
			Type:    xdr.PublicKeyTypePublicKeyTypeEd25519,
			Ed25519: &emptyAcc,
		},
		Operations: []xdr.Operation{
			{
				Body: xdr.OperationBody{
					Type: xdr.OperationTypePayment,
					PaymentOp: &xdr.PaymentOp{
						Destination: xdr.AccountId{
							Type:    xdr.PublicKeyTypePublicKeyTypeEd25519,
							Ed25519: &emptyAcc,
						},
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
}

func setup(t *testing.T) (env testEnv, cleanup func()) {
	conn, serv, err := testutil.NewServer()
	require.NoError(t, err)

	env.client = transactionpb.NewTransactionClient(conn)
	env.hClient = &horizon.MockClient{}
	env.hClientV2 = &horizonclient.MockClient{}

	s := New(memory.New(), static.New(), env.hClient, env.hClientV2)
	serv.RegisterService(func(server *grpc.Server) {
		transactionpb.RegisterTransactionServer(server, s)
	})

	cleanup, err = serv.Serve()
	require.NoError(t, err)

	return env, cleanup
}

func TestSubmitSend_Happy(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	txnBytes, err := emptyTxn.MarshalBinary()
	require.NoError(t, err)

	hashBytes := sha256.Sum256(txnBytes)

	horizonResult := horizonprotocols.TransactionSuccess{
		Hash:   hex.EncodeToString(hashBytes[:]),
		Ledger: 10,
		Result: base64.StdEncoding.EncodeToString([]byte("test")),
	}
	env.hClient.On("SubmitTransaction", mock.AnythingOfType("string")).Return(horizonResult, nil).Once()

	resp, err := env.client.SubmitSend(context.Background(), &transactionpb.SubmitSendRequest{
		TransactionXdr: txnBytes,
	})

	assert.NoError(t, err)
	assert.EqualValues(t, horizonResult.Ledger, resp.Ledger)
	assert.EqualValues(t, horizonResult.Hash, hex.EncodeToString(resp.Hash.Value))
	assert.EqualValues(t, horizonResult.Result, base64.StdEncoding.EncodeToString(resp.ResultXdr))
}

func TestSubmitSend_Invalid(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	invalidRequests := []*transactionpb.SubmitSendRequest{
		{},
		{
			TransactionXdr: []byte{1, 2},
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

		txnBytes, err := txn.MarshalBinary()
		require.NoError(t, err)

		invalidRequests = append(invalidRequests, &transactionpb.SubmitSendRequest{
			TransactionXdr: txnBytes,
		})
	*/

	for _, r := range invalidRequests {
		_, err := env.client.SubmitSend(context.Background(), r)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	}
}

func TestSubmit_HorizonErrors(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	txnBytes, err := emptyTxn.MarshalBinary()
	require.NoError(t, err)

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
		_, err := env.client.SubmitSend(context.Background(), &transactionpb.SubmitSendRequest{
			TransactionXdr: txnBytes,
		})
		assert.Equal(t, tc.grpcCode, status.Code(err))
	}
}

func TestGetTransaction_Happy(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	txnBytes, err := emptyTxn.MarshalBinary()
	require.NoError(t, err)

	hashBytes := sha256.Sum256(txnBytes)

	horizonResult := horizonprotocols.Transaction{
		Hash:        hex.EncodeToString(hashBytes[:]),
		Ledger:      10,
		ResultXdr:   base64.StdEncoding.EncodeToString([]byte("result")),
		EnvelopeXdr: base64.StdEncoding.EncodeToString([]byte("envelope")),
	}

	env.hClient.On("LoadTransaction", mock.AnythingOfType("string")).Return(horizonResult, nil).Once()
	resp, err := env.client.GetTransaction(context.Background(), &transactionpb.GetTransactionRequest{
		TransactionHash: &commonpb.TransactionHash{
			Value: hashBytes[:],
		},
	})
	require.NoError(t, err)

	assert.EqualValues(t, horizonResult.Ledger, resp.Ledger)
	assert.Equal(t, transactionpb.GetTransactionResponse_SUCCESS, resp.State)
	assert.NotNil(t, resp.Item)
	assert.Equal(t, horizonResult.Hash, hex.EncodeToString(resp.Item.Hash.Value))
	assert.Equal(t, horizonResult.ResultXdr, base64.StdEncoding.EncodeToString(resp.Item.ResultXdr))
	assert.Equal(t, horizonResult.EnvelopeXdr, base64.StdEncoding.EncodeToString(resp.Item.EnvelopeXdr))
	assert.Nil(t, resp.Item.AgoraDataUrl)
	assert.Nil(t, resp.Item.AgoraData)
}

func TestGetTransaction_HorizonErrors(t *testing.T) {
	env, cleanup := setup(t)
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
	env, cleanup := setup(t)
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
		assert.Nil(t, item.AgoraData)
		assert.Nil(t, item.AgoraDataUrl)

		require.Len(t, env.hClientV2.Calls, i+1)
		txnReq := env.hClientV2.Calls[i].Arguments[0].(horizonclient.TransactionRequest)
		assert.Equal(t, tc.cursor, txnReq.Cursor)
		assert.Equal(t, tc.direction, txnReq.Order)
	}
}

func TestGetHistory_HorizonErrors(t *testing.T) {
	env, cleanup := setup(t)
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
