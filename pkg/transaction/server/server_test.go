package server

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/agora-common/headers"
	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/agora-common/solana"
	solanamemo "github.com/kinecosystem/agora-common/solana/memo"
	"github.com/kinecosystem/agora-common/solana/token"
	agoratestutil "github.com/kinecosystem/agora-common/testutil"
	"github.com/kinecosystem/agora-common/webhook/signtransaction"
	"github.com/kinecosystem/go/clients/horizon"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v4"
	commonpbv3 "github.com/kinecosystem/agora-api/genproto/common/v3"
	"github.com/kinecosystem/agora-api/genproto/common/v4"
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v4"
	transactionpb "github.com/kinecosystem/agora-api/genproto/transaction/v4"

	"github.com/kinecosystem/agora/pkg/account/info"
	infomemory "github.com/kinecosystem/agora/pkg/account/info/memory"
	"github.com/kinecosystem/agora/pkg/account/specstate"
	"github.com/kinecosystem/agora/pkg/app"
	appconfigmemory "github.com/kinecosystem/agora/pkg/app/memory"
	"github.com/kinecosystem/agora/pkg/events"
	"github.com/kinecosystem/agora/pkg/events/eventspb"
	eventsmemory "github.com/kinecosystem/agora/pkg/events/memory"
	"github.com/kinecosystem/agora/pkg/invoice"
	invoicedb "github.com/kinecosystem/agora/pkg/invoice/memory"
	"github.com/kinecosystem/agora/pkg/testutil"
	"github.com/kinecosystem/agora/pkg/transaction"
	"github.com/kinecosystem/agora/pkg/transaction/dedupe"
	dedupememory "github.com/kinecosystem/agora/pkg/transaction/dedupe/memory"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
	ingestionmemory "github.com/kinecosystem/agora/pkg/transaction/history/ingestion/memory"
	historymemory "github.com/kinecosystem/agora/pkg/transaction/history/memory"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
	historytestutil "github.com/kinecosystem/agora/pkg/transaction/history/model/testutil"
)

type serverEnv struct {
	token      ed25519.PublicKey
	subsidizer ed25519.PrivateKey
	server     *server
	client     transactionpb.TransactionClient

	sc               *solana.MockClient
	invoiceStore     invoice.Store
	appConfig        app.ConfigStore
	rw               *historymemory.RW
	committer        ingestion.Committer
	authorizer       *mockAuthorizer
	infoCache        info.Cache
	webhookSubmitter *mockSubmitter
	streamSubmitter  events.Submitter
	deduper          dedupe.Deduper

	hClient *horizon.MockClient

	mu     sync.Mutex
	events []*eventspb.Event
}

type mockAuthorizer struct {
	mock.Mock
}

func (m *mockAuthorizer) Authorize(ctx context.Context, tx solana.Transaction, il *commonpbv3.InvoiceList, ignoreSigned bool) (transaction.Authorization, error) {
	args := m.Called(ctx, tx, (*commonpbv3.InvoiceList)(il), ignoreSigned)
	return args.Get(0).(transaction.Authorization), args.Error(1)
}

type mockSubmitter struct {
	mock.Mock
}

func (m *mockSubmitter) Submit(ctx context.Context, e *model.Entry) error {
	args := m.Called(ctx, e)
	return args.Error(0)
}

func setupServerEnv(t *testing.T) (env *serverEnv, cleanup func()) {
	conn, serv, err := agoratestutil.NewServer(
		agoratestutil.WithUnaryServerInterceptor(headers.UnaryServerInterceptor()),
		agoratestutil.WithStreamServerInterceptor(headers.StreamServerInterceptor()),
	)
	require.NoError(t, err)

	env = &serverEnv{}

	env.client = transactionpb.NewTransactionClient(conn)
	env.sc = solana.NewMockClient()
	env.invoiceStore = invoicedb.New()
	env.rw = historymemory.New()
	env.appConfig = appconfigmemory.New()
	env.committer = ingestionmemory.New()
	env.authorizer = &mockAuthorizer{}
	env.infoCache, err = infomemory.NewCache(5*time.Second, 5*time.Second, 1000)
	require.NoError(t, err)
	env.webhookSubmitter = &mockSubmitter{}
	env.streamSubmitter = eventsmemory.New(func(e *eventspb.Event) {
		env.mu.Lock()
		env.events = append(env.events, proto.Clone(e).(*eventspb.Event))
		env.mu.Unlock()
	})
	env.deduper = dedupememory.New()

	env.subsidizer = testutil.GenerateSolanaKeypair(t)
	env.token = testutil.GenerateSolanaKeypair(t).Public().(ed25519.PublicKey)

	env.hClient = &horizon.MockClient{}
	// Required for migrating transfer account pairs
	env.hClient.On("LoadAccount", mock.Anything).Return(*testutil.GenerateHorizonAccount("", "100", "1"), nil)

	s := New(
		env.sc,
		env.invoiceStore,
		env.rw,
		env.appConfig,
		env.committer,
		env.authorizer,
		env.webhookSubmitter,
		env.streamSubmitter,
		env.deduper,
		specstate.NewSpeculativeLoader(token.NewClient(env.sc, env.token), env.infoCache),
		env.token,
		env.subsidizer,
	)
	env.server = s.(*server)

	serv.RegisterService(func(server *grpc.Server) {
		transactionpb.RegisterTransactionServer(server, s)
	})

	cleanup, err = serv.Serve()
	require.NoError(t, err)

	return env, cleanup
}

func TestGetServiceConfig(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	// No headers
	resp, err := env.client.GetServiceConfig(context.Background(), &transactionpb.GetServiceConfigRequest{})
	assert.NoError(t, err)
	assert.EqualValues(t, token.ProgramKey, resp.TokenProgram.Value)
	assert.EqualValues(t, env.token, resp.Token.Value)
	assert.EqualValues(t, env.subsidizer.Public().(ed25519.PublicKey), resp.SubsidizerAccount.Value)

	// Headers, no mapped config
	md := map[string]string{
		"app-index": "1",
	}
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.New(md))
	resp, err = env.client.GetServiceConfig(ctx, &transactionpb.GetServiceConfigRequest{})
	assert.NoError(t, err)
	assert.EqualValues(t, token.ProgramKey, resp.TokenProgram.Value)
	assert.EqualValues(t, env.token, resp.Token.Value)
	assert.EqualValues(t, env.subsidizer.Public().(ed25519.PublicKey), resp.SubsidizerAccount.Value)

	// Headers, with a mapped config
	customSub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	require.NoError(t, env.appConfig.Add(context.Background(), 1, &app.Config{
		AppName:    "test",
		Subsidizer: customSub,
	}))
	resp, err = env.client.GetServiceConfig(ctx, &transactionpb.GetServiceConfigRequest{})
	assert.NoError(t, err)
	assert.EqualValues(t, token.ProgramKey, resp.TokenProgram.Value)
	assert.EqualValues(t, env.token, resp.Token.Value)
	assert.EqualValues(t, customSub, resp.SubsidizerAccount.Value)
}

func TestGetMinimumKinVersion(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	resp, err := env.client.GetMinimumKinVersion(context.Background(), &transactionpb.GetMinimumKinVersionRequest{})
	assert.NoError(t, err)
	assert.EqualValues(t, 4, resp.Version)

	md := map[string]string{
		"desired-kin-version": "2",
	}
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.New(md))
	resp, err = env.client.GetMinimumKinVersion(ctx, &transactionpb.GetMinimumKinVersionRequest{})
	assert.NoError(t, err)
	assert.EqualValues(t, 2, resp.Version)

	md = map[string]string{
		"kin-version": "2",
	}
	ctx = metadata.NewOutgoingContext(context.Background(), metadata.New(md))
	resp, err = env.client.GetMinimumKinVersion(ctx, &transactionpb.GetMinimumKinVersionRequest{})
	assert.NoError(t, err)
	assert.EqualValues(t, 4, resp.Version)
}

func TestGetRecentBlockhash(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	var blockhash solana.Blockhash
	copy(blockhash[:], bytes.Repeat([]byte{1}, 32))
	env.sc.On("GetRecentBlockhash").Return(blockhash, nil)

	resp, err := env.client.GetRecentBlockhash(context.Background(), &transactionpb.GetRecentBlockhashRequest{})
	assert.NoError(t, err)
	assert.EqualValues(t, blockhash[:], resp.Blockhash.Value)
}

func TestGetMinimumBalanceForRentExemption(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	env.sc.On("GetMinimumBalanceForRentExemption", uint64(10)).Return(uint64(32), nil)

	resp, err := env.client.GetMinimumBalanceForRentExemption(context.Background(), &transactionpb.GetMinimumBalanceForRentExemptionRequest{
		Size: 10,
	})
	assert.NoError(t, err)
	assert.EqualValues(t, 32, resp.Lamports)
}

func TestGetTransaction_Stellar(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	accounts := testutil.GenerateAccountIDs(t, 10)
	entry, id := historytestutil.GenerateStellarEntry(t, 10, 10, accounts[0], accounts[1:], nil, nil)

	resp, err := env.client.GetTransaction(context.Background(), &transactionpb.GetTransactionRequest{
		TransactionId: &common.TransactionId{
			Value: id,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.GetTransactionResponse_UNKNOWN, resp.State)

	assert.NoError(t, env.rw.Write(context.Background(), entry))

	resp, err = env.client.GetTransaction(context.Background(), &transactionpb.GetTransactionRequest{
		TransactionId: &common.TransactionId{
			Value: id,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.GetTransactionResponse_SUCCESS, resp.State)
	assert.EqualValues(t, 0, resp.Confirmations)
	assert.EqualValues(t, 0, resp.Slot)
	assert.Equal(t, entry.GetStellar().EnvelopeXdr, resp.Item.GetStellarTransaction().EnvelopeXdr)
}

func TestGetTransaction_Solana(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	sender := testutil.GenerateSolanaKeypair(t)
	receivers := testutil.GenerateSolanaKeys(t, 5)
	entry, id := historytestutil.GenerateSolanaEntry(t, 10, true, sender, receivers, nil, nil)

	env.sc.On("GetConfirmedTransaction", mock.Anything, mock.Anything).Return(solana.ConfirmedTransaction{}, solana.ErrSignatureNotFound)

	resp, err := env.client.GetTransaction(context.Background(), &transactionpb.GetTransactionRequest{
		TransactionId: &common.TransactionId{
			Value: id,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.GetTransactionResponse_UNKNOWN, resp.State)

	assert.NoError(t, env.rw.Write(context.Background(), entry))

	resp, err = env.client.GetTransaction(context.Background(), &transactionpb.GetTransactionRequest{
		TransactionId: &common.TransactionId{
			Value: id,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.GetTransactionResponse_SUCCESS, resp.State)
	assert.EqualValues(t, 0, resp.Confirmations)
	assert.EqualValues(t, 10, resp.Slot)
	assert.Equal(t, entry.GetSolana().Transaction, resp.Item.GetSolanaTransaction().Value)
}

func TestGetHistory(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	sender := testutil.GenerateSolanaKeypair(t)
	receivers := testutil.GenerateSolanaKeys(t, 5)

	// Since we always pull history from the identity account, it's ok
	// that we haven't mapped any of accounts here. We test separately
	// to ensure account resolution works for solana.
	env.sc.On("GetTokenAccountsByOwner", mock.Anything, env.token).Return([]ed25519.PublicKey{}, nil)

	var entry *model.Entry
	for i := 0; i < 10; i++ {
		entry, _ = historytestutil.GenerateSolanaEntry(t, uint64(i+1), true, sender, receivers, nil, nil)
		assert.NoError(t, env.rw.Write(context.Background(), entry))
	}

	latest := historytestutil.GetOrderingKey(t, entry)
	assert.NoError(t, env.committer.Commit(context.Background(), ingestion.GetHistoryIngestorName(model.KinVersion_KIN4), nil, latest))

	resp, err := env.client.GetHistory(context.Background(), &transactionpb.GetHistoryRequest{
		AccountId: &common.SolanaAccountId{
			Value: sender.Public().(ed25519.PublicKey),
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.GetHistoryResponse_OK, resp.Result)
	assert.Len(t, resp.Items, 10)

	respReversed, err := env.client.GetHistory(context.Background(), &transactionpb.GetHistoryRequest{
		AccountId: &common.SolanaAccountId{
			Value: sender.Public().(ed25519.PublicKey),
		},
		Direction: transactionpb.GetHistoryRequest_DESC,
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.GetHistoryResponse_OK, resp.Result)
	assert.Len(t, resp.Items, 10)
	for i := 0; i < len(resp.Items); i++ {
		assert.True(t, proto.Equal(resp.Items[i], respReversed.Items[len(respReversed.Items)-1-i]))
	}

	respSliced, err := env.client.GetHistory(context.Background(), &transactionpb.GetHistoryRequest{
		AccountId: &common.SolanaAccountId{
			Value: sender.Public().(ed25519.PublicKey),
		},
		Cursor: resp.Items[4].Cursor,
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.GetHistoryResponse_OK, resp.Result)
	assert.Len(t, respSliced.Items, 5)
	for i := 0; i < len(respSliced.Items); i++ {
		assert.True(t, proto.Equal(resp.Items[i+5], respSliced.Items[i]))
	}
}

func TestSignTransaction(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	txn, _ := generateTransaction(t, env.subsidizer.Public().(ed25519.PublicKey), 1, 0, nil, nil)
	auth := transaction.Authorization{
		Result: transaction.AuthorizationResultOK,
		SignResponse: &signtransaction.SuccessResponse{
			Signature: ed25519.Sign(env.subsidizer, txn.Message.Marshal()),
		},
	}
	env.authorizer.On("Authorize", mock.Anything, txn, (*commonpbv3.InvoiceList)(nil), false).Return(auth, nil)

	resp, err := env.client.SignTransaction(context.Background(), &transactionpb.SignTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.SignTransactionResponse_OK, resp.Result)
	assert.Equal(t, auth.SignResponse.Signature, resp.Signature.Value)
	assert.Empty(t, env.rw.Writes)

	// Neither the solana client or the webhooks should have been called.
	env.sc.Mock.AssertExpectations(t)
	env.webhookSubmitter.Mock.AssertExpectations(t)
}

func TestSignTransaction_Rejected(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	txn, _ := generateTransaction(t, env.subsidizer.Public().(ed25519.PublicKey), 1, 0, nil, nil)
	auth := transaction.Authorization{
		Result: transaction.AuthorizationResultRejected,
	}
	env.authorizer.On("Authorize", mock.Anything, txn, (*commonpbv3.InvoiceList)(nil), false).Return(auth, nil)

	resp, err := env.client.SignTransaction(context.Background(), &transactionpb.SignTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.SignTransactionResponse_REJECTED, resp.Result)
	assert.Nil(t, resp.Signature)
	assert.Nil(t, resp.InvoiceErrors)

	// Neither the solana client or the webhooks should have been called.
	env.sc.Mock.AssertExpectations(t)
	env.webhookSubmitter.Mock.AssertExpectations(t)
}

func TestSignTransaction_PayerRequired(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	txn, _ := generateTransaction(t, env.subsidizer.Public().(ed25519.PublicKey), 1, 0, nil, nil)
	auth := transaction.Authorization{
		Result: transaction.AuthorizationResultPayerRequired,
	}
	env.authorizer.On("Authorize", mock.Anything, txn, (*commonpbv3.InvoiceList)(nil), false).Return(auth, nil)

	resp, err := env.client.SignTransaction(context.Background(), &transactionpb.SignTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.SignTransactionResponse_REJECTED, resp.Result)
	assert.Nil(t, resp.Signature)
	assert.Nil(t, resp.InvoiceErrors)

	// Neither the solana client or the webhooks should have been called.
	env.sc.Mock.AssertExpectations(t)
	env.webhookSubmitter.Mock.AssertExpectations(t)
}

func TestSignTransaction_InvoiceErrors(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	txn, _ := generateTransaction(t, env.subsidizer.Public().(ed25519.PublicKey), 1, 0, nil, nil)
	auth := transaction.Authorization{
		Result: transaction.AuthorizationResultInvoiceError,
		InvoiceErrors: []*commonpbv3.InvoiceError{
			{
				OpIndex: 0,
				Reason:  commonpbv3.InvoiceError_ALREADY_PAID,
				Invoice: &commonpbv3.Invoice{
					Items: []*commonpbv3.Invoice_LineItem{
						{
							Title: "test1",
						},
					},
				},
			},
			{
				OpIndex: 1,
				Invoice: &commonpbv3.Invoice{
					Items: []*commonpbv3.Invoice_LineItem{
						{
							Title: "test2",
						},
					},
				},
			},
		},
	}
	env.authorizer.On("Authorize", mock.Anything, txn, (*commonpbv3.InvoiceList)(nil), false).Return(auth, nil)

	resp, err := env.client.SignTransaction(context.Background(), &transactionpb.SignTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.SignTransactionResponse_INVOICE_ERROR, resp.Result)
	assert.Nil(t, resp.Signature)
	assert.Equal(t, len(auth.InvoiceErrors), len(resp.InvoiceErrors))
	for i := range auth.InvoiceErrors {
		assert.True(t, proto.Equal(auth.InvoiceErrors[i], resp.InvoiceErrors[i]))
	}

	// Neither the solana client or the webhooks should have been called.
	env.sc.Mock.AssertExpectations(t)
	env.webhookSubmitter.Mock.AssertExpectations(t)
}

func TestSubmitTransaction_Plain(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	txn, accounts := generateTransaction(t, env.subsidizer.Public().(ed25519.PublicKey), 1, 0, nil, nil)

	auth := transaction.Authorization{
		Result: transaction.AuthorizationResultOK,
		SignResponse: &signtransaction.SuccessResponse{
			Signature: ed25519.Sign(env.subsidizer, txn.Message.Marshal()),
		},
	}
	env.authorizer.On("Authorize", mock.Anything, txn, (*commonpbv3.InvoiceList)(nil), true).Return(auth, nil)
	env.webhookSubmitter.On("Submit", mock.Anything, mock.Anything).Return(nil).Times(1)

	senderInfo := token.Account{
		Mint:   env.token,
		Owner:  accounts[0],
		Amount: 10,
		State:  token.AccountStateInitialized,
	}
	receiverInfo := token.Account{
		Mint:   env.token,
		Owner:  accounts[1],
		Amount: 10,
		State:  token.AccountStateInitialized,
	}
	accountInfos := []solana.AccountInfo{
		{
			Owner: token.ProgramKey,
			Data:  senderInfo.Marshal(),
		},
		{
			Owner: token.ProgramKey,
			Data:  receiverInfo.Marshal(),
		},
	}

	env.sc.On("GetAccountInfo", accounts[0], mock.Anything).Return(accountInfos[0], nil)
	env.sc.On("GetAccountInfo", accounts[1], mock.Anything).Return(accountInfos[1], nil)

	var sig solana.Signature
	copy(sig[:], ed25519.Sign(env.subsidizer, txn.Message.Marshal()))

	var submitted solana.Transaction
	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentRoot).
		Run(func(args mock.Arguments) {
			submitted = args.Get(0).(solana.Transaction)
		}).
		Return(sig, &solana.SignatureStatus{}, nil)

	for _, a := range accounts {
		info := &accountpb.AccountInfo{
			AccountId: &commonpb.SolanaAccountId{
				Value: a,
			},
			Balance: 10,
		}
		assert.NoError(t, env.infoCache.Put(context.Background(), info))
	}

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
		Commitment: common.Commitment_ROOT,
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.SubmitTransactionResponse_OK, resp.Result)
	assert.Equal(t, sig[:], resp.Signature.Value)
	assert.Equal(t, submitted.Signatures[0][:], resp.Signature.Value)
	assert.Len(t, env.rw.Writes, 1)

	assert.NoError(t, txn.Sign(env.subsidizer))

	// Ensure speculative updates is working
	for i, a := range accounts {
		info, err := env.infoCache.Get(context.Background(), a)
		assert.NoError(t, err)

		if i == 0 {
			assert.EqualValues(t, 9, info.Balance)
		} else {
			assert.EqualValues(t, 11, info.Balance)
		}
	}

	env.webhookSubmitter.AssertExpectations(t)
	submittedEntry := env.webhookSubmitter.Calls[0].Arguments.Get(1).(*model.Entry)
	assert.EqualValues(t, txn.Marshal(), submittedEntry.GetSolana().Transaction)
	require.Equal(t, 1, len(env.events))
	assert.Equal(t, txn.Marshal(), env.events[0].GetTransactionEvent().Transaction)
}

func TestSubmitTransaction_AlreadySigned(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	txn, accounts := generateTransaction(t, env.subsidizer.Public().(ed25519.PublicKey), 1, 0, nil, nil)
	require.NoError(t, txn.Sign(env.subsidizer))
	env.webhookSubmitter.On("Submit", mock.Anything, mock.Anything).Return(nil).Times(1)

	senderInfo := token.Account{
		Mint:   env.token,
		Owner:  accounts[0],
		Amount: 10,
		State:  token.AccountStateInitialized,
	}
	receiverInfo := token.Account{
		Mint:   env.token,
		Owner:  accounts[1],
		Amount: 10,
		State:  token.AccountStateInitialized,
	}
	accountInfos := []solana.AccountInfo{
		{
			Owner: token.ProgramKey,
			Data:  senderInfo.Marshal(),
		},
		{
			Owner: token.ProgramKey,
			Data:  receiverInfo.Marshal(),
		},
	}

	env.sc.On("GetAccountInfo", accounts[0], mock.Anything).Return(accountInfos[0], nil)
	env.sc.On("GetAccountInfo", accounts[1], mock.Anything).Return(accountInfos[1], nil)

	var sig solana.Signature
	copy(sig[:], ed25519.Sign(env.subsidizer, txn.Message.Marshal()))

	var submitted solana.Transaction
	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentRoot).
		Run(func(args mock.Arguments) {
			submitted = args.Get(0).(solana.Transaction)
		}).
		Return(sig, &solana.SignatureStatus{}, nil)

	for _, a := range accounts {
		info := &accountpb.AccountInfo{
			AccountId: &commonpb.SolanaAccountId{
				Value: a,
			},
			Balance: 10,
		}
		assert.NoError(t, env.infoCache.Put(context.Background(), info))
	}

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
		Commitment: common.Commitment_ROOT,
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.SubmitTransactionResponse_OK, resp.Result)
	assert.Equal(t, sig[:], resp.Signature.Value)
	assert.Equal(t, submitted.Signatures[0][:], resp.Signature.Value)
	assert.Len(t, env.rw.Writes, 1)

	assert.NoError(t, txn.Sign(env.subsidizer))

	// Ensure speculative updates is working
	for i, a := range accounts {
		info, err := env.infoCache.Get(context.Background(), a)
		assert.NoError(t, err)

		if i == 0 {
			assert.EqualValues(t, 9, info.Balance)
		} else {
			assert.EqualValues(t, 11, info.Balance)
		}
	}

	env.webhookSubmitter.AssertExpectations(t)
	submittedEntry := env.webhookSubmitter.Calls[0].Arguments.Get(1).(*model.Entry)
	assert.EqualValues(t, txn.Marshal(), submittedEntry.GetSolana().Transaction)
	require.Equal(t, 1, len(env.events))
	assert.Equal(t, txn.Marshal(), env.events[0].GetTransactionEvent().Transaction)
}

func TestSubmitTransaction_CloseAccount(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	txn, accounts := generateTransaction(t, env.subsidizer.Public().(ed25519.PublicKey), 4, 2, nil, nil)

	auth := transaction.Authorization{
		Result: transaction.AuthorizationResultOK,
		SignResponse: &signtransaction.SuccessResponse{
			Signature: ed25519.Sign(env.subsidizer, txn.Message.Marshal()),
		},
	}
	env.authorizer.On("Authorize", mock.Anything, txn, (*commonpbv3.InvoiceList)(nil), true).Return(auth, nil)
	env.webhookSubmitter.On("Submit", mock.Anything, mock.Anything).Return(nil).Times(1)

	for i := 0; i < 5; i++ {
		receiverInfo := token.Account{
			Mint:   env.token,
			Owner:  accounts[i],
			Amount: 20,
			State:  token.AccountStateInitialized,
		}
		accountInfo := solana.AccountInfo{
			Owner: token.ProgramKey,
			Data:  receiverInfo.Marshal(),
		}

		env.sc.On("GetAccountInfo", accounts[i], mock.Anything).Return(accountInfo, nil)
	}

	var sig solana.Signature
	copy(sig[:], ed25519.Sign(env.subsidizer, txn.Message.Marshal()))

	var submitted solana.Transaction
	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentRoot).
		Run(func(args mock.Arguments) {
			submitted = args.Get(0).(solana.Transaction)
		}).
		Return(sig, &solana.SignatureStatus{}, nil)

	for _, a := range accounts {
		pub := make(ed25519.PublicKey, ed25519.PublicKeySize)
		copy(pub, a)
		info := &accountpb.AccountInfo{
			AccountId: &commonpb.SolanaAccountId{
				Value: pub,
			},
			Balance: 10,
		}
		assert.NoError(t, env.infoCache.Put(context.Background(), info))
	}

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
		Commitment: common.Commitment_ROOT,
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.SubmitTransactionResponse_OK, resp.Result)
	assert.Equal(t, sig[:], resp.Signature.Value)
	assert.Equal(t, submitted.Signatures[0][:], resp.Signature.Value)
	assert.Len(t, env.rw.Writes, 1)

	assert.NoError(t, txn.Sign(env.subsidizer))

	// Ensure speculative updates is working
	for i, a := range accounts {
		info, err := env.infoCache.Get(context.Background(), a)
		assert.NoError(t, err)

		if i == 0 {
			assert.EqualValues(t, 10-(1+2+3+4), info.Balance)
		} else {
			assert.EqualValues(t, 10+i, info.Balance)
		}
	}

	env.webhookSubmitter.AssertExpectations(t)
	submittedEntry := env.webhookSubmitter.Calls[0].Arguments.Get(1).(*model.Entry)
	assert.EqualValues(t, txn.Marshal(), submittedEntry.GetSolana().Transaction)
	require.Equal(t, 1, len(env.events))
	assert.Equal(t, txn.Marshal(), env.events[0].GetTransactionEvent().Transaction)
}

func TestSubmitTransaction_CreateAndFund(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	keys := testutil.GenerateSolanaKeys(t, 3)

	create, account, err := token.CreateAssociatedTokenAccount(
		env.subsidizer.Public().(ed25519.PublicKey),
		keys[0],
		env.token,
	)
	require.NoError(t, err)
	txn := solana.NewTransaction(
		env.subsidizer.Public().(ed25519.PublicKey),
		create,
		token.Transfer(
			keys[2],
			account,
			keys[1],
			7,
		),
	)

	auth := transaction.Authorization{
		Result: transaction.AuthorizationResultOK,
		SignResponse: &signtransaction.SuccessResponse{
			Signature: ed25519.Sign(env.subsidizer, txn.Message.Marshal()),
		},
	}

	senderInfo := token.Account{
		Mint:   env.token,
		Owner:  keys[2],
		Amount: 10,
		State:  token.AccountStateInitialized,
	}
	receiverInfo := token.Account{
		Mint:   env.token,
		Owner:  account,
		Amount: 0,
		State:  token.AccountStateInitialized,
	}
	accountInfos := []solana.AccountInfo{
		{
			Owner: token.ProgramKey,
			Data:  senderInfo.Marshal(),
		},
		{
			Owner: token.ProgramKey,
			Data:  receiverInfo.Marshal(),
		},
	}

	var sig solana.Signature
	var submitted solana.Transaction
	copy(sig[:], ed25519.Sign(env.subsidizer, txn.Message.Marshal()))
	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentRoot).
		Run(func(args mock.Arguments) {
			submitted = args.Get(0).(solana.Transaction)
		}).
		Return(sig, &solana.SignatureStatus{}, nil)

	env.authorizer.On("Authorize", mock.Anything, txn, (*commonpbv3.InvoiceList)(nil), true).Return(auth, nil)
	env.webhookSubmitter.On("Submit", mock.Anything, mock.Anything).Return(nil)
	env.sc.On("GetAccountInfo", keys[2], mock.Anything).Return(accountInfos[0], nil)
	env.sc.On("GetAccountInfo", account, mock.Anything).Return(accountInfos[1], nil)

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
		Commitment: common.Commitment_ROOT,
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.SubmitTransactionResponse_OK, resp.Result)
	assert.Equal(t, sig[:], resp.Signature.Value)
	assert.Equal(t, submitted.Signatures[0][:], resp.Signature.Value)
	assert.Len(t, env.rw.Writes, 1)

	assert.NoError(t, txn.Sign(env.subsidizer))

	// Ensure speculative updates is working
	info, err := env.infoCache.Get(context.Background(), keys[2])
	assert.NoError(t, err)
	assert.EqualValues(t, 3, info.Balance)
	info, err = env.infoCache.Get(context.Background(), account)
	assert.NoError(t, err)
	assert.EqualValues(t, 7, info.Balance)

	env.webhookSubmitter.AssertExpectations(t)
	submittedEntry := env.webhookSubmitter.Calls[0].Arguments.Get(1).(*model.Entry)
	assert.EqualValues(t, txn.Marshal(), submittedEntry.GetSolana().Transaction)
	require.Equal(t, 1, len(env.events))
	assert.Equal(t, txn.Marshal(), env.events[0].GetTransactionEvent().Transaction)
}

func TestSubmitTransaction_DuplicateSignature(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	txn, _ := generateTransaction(t, env.subsidizer.Public().(ed25519.PublicKey), 1, 0, nil, nil)

	auth := transaction.Authorization{
		Result: transaction.AuthorizationResultOK,
		SignResponse: &signtransaction.SuccessResponse{
			Signature: ed25519.Sign(env.subsidizer, txn.Message.Marshal()),
		},
	}
	env.authorizer.On("Authorize", mock.Anything, txn, (*commonpbv3.InvoiceList)(nil), true).Return(auth, nil)
	env.webhookSubmitter.On("Submit", mock.Anything, mock.Anything).Return(nil)
	env.sc.On("GetAccountInfo", mock.Anything, mock.Anything).Return(solana.AccountInfo{}, nil)

	var sig solana.Signature
	copy(sig[:], ed25519.Sign(env.subsidizer, txn.Message.Marshal()))

	sigStatus := &solana.SignatureStatus{
		ErrorResult: solana.NewTransactionError(solana.TransactionErrorDuplicateSignature),
	}

	var submitted solana.Transaction
	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentRoot).
		Run(func(args mock.Arguments) {
			submitted = args.Get(0).(solana.Transaction)
		}).
		Return(sig, sigStatus, nil)

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
		Commitment: common.Commitment_ROOT,
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.SubmitTransactionResponse_ALREADY_SUBMITTED, resp.Result)
	assert.Equal(t, sig[:], resp.Signature.Value)
	assert.Equal(t, submitted.Signatures[0][:], resp.Signature.Value)
	assert.Len(t, env.rw.Writes, 1)

	assert.NoError(t, txn.Sign(env.subsidizer))

	// Update the entry information, and then submit again.
	//
	// This tests the history.ErrInvalidUpdate handling
	entry, err := env.rw.GetTransaction(context.Background(), resp.Signature.Value)
	assert.NoError(t, err)
	entry.GetSolana().Slot = 10
	assert.NoError(t, env.rw.Write(context.Background(), entry))

	resp, err = env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
		Commitment: common.Commitment_ROOT,
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.SubmitTransactionResponse_ALREADY_SUBMITTED, resp.Result)
	assert.Equal(t, sig[:], resp.Signature.Value)
	assert.Equal(t, submitted.Signatures[0][:], resp.Signature.Value)
	assert.Len(t, env.rw.Writes, 2)
}

func TestSubmitTransaction_DedupeSuccess(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	txn, accounts := generateTransaction(t, env.subsidizer.Public().(ed25519.PublicKey), 1, 0, nil, nil)

	auth := transaction.Authorization{
		Result: transaction.AuthorizationResultOK,
		SignResponse: &signtransaction.SuccessResponse{
			Signature: ed25519.Sign(env.subsidizer, txn.Marshal()),
		},
	}
	env.authorizer.On("Authorize", mock.Anything, txn, (*commonpbv3.InvoiceList)(nil), true).Return(auth, nil)
	env.webhookSubmitter.On("Submit", mock.Anything, mock.Anything).Return(nil).Once()
	env.sc.On("GetAccountInfo", mock.Anything, mock.Anything).Return(solana.AccountInfo{}, nil)

	var sig solana.Signature
	copy(sig[:], ed25519.Sign(env.subsidizer, txn.Message.Marshal()))

	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentRoot).Return(sig, &solana.SignatureStatus{}, nil).Once()

	for _, a := range accounts {
		info := &accountpb.AccountInfo{
			AccountId: &commonpb.SolanaAccountId{
				Value: a,
			},
			Balance: 10,
		}
		assert.NoError(t, env.infoCache.Put(context.Background(), info))
	}

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
		Commitment: common.Commitment_ROOT,
		DedupeId:   []byte("dupe1"),
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.SubmitTransactionResponse_OK, resp.Result)
	require.NotEmpty(t, sig[:], resp.Signature.Value)

	resp, err = env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
		Commitment: common.Commitment_ROOT,
		DedupeId:   []byte("dupe1"),
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.SubmitTransactionResponse_OK, resp.Result)
	require.NotEmpty(t, sig[:], resp.Signature.Value)
}

func TestSubmitTransaction_DedupeFailed(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	txn, accounts := generateTransaction(t, env.subsidizer.Public().(ed25519.PublicKey), 1, 0, nil, nil)

	// Since the calls fail, we expect that retries at a higher level go through.
	auth := transaction.Authorization{
		Result: transaction.AuthorizationResultOK,
		SignResponse: &signtransaction.SuccessResponse{
			Signature: ed25519.Sign(env.subsidizer, txn.Message.Marshal()),
		},
	}
	env.authorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything, true).Return(auth, nil).Times(4)

	var sig solana.Signature
	copy(sig[:], ed25519.Sign(env.subsidizer, txn.Message.Marshal()))

	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentRoot).Return(sig, &solana.SignatureStatus{}, errors.New("unexpected")).Times(2)
	env.sc.On("GetAccountInfo", mock.Anything, mock.Anything).Return(solana.AccountInfo{}, nil)

	for _, a := range accounts {
		info := &accountpb.AccountInfo{
			AccountId: &commonpb.SolanaAccountId{
				Value: a,
			},
			Balance: 10,
		}
		assert.NoError(t, env.infoCache.Put(context.Background(), info))
	}

	for i := 0; i < 2; i++ {
		_, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
			Transaction: &commonpb.Transaction{
				Value: txn.Marshal(),
			},
			Commitment: common.Commitment_ROOT,
			DedupeId:   []byte("failed"),
		})
		assert.Error(t, err)
	}

	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentMax).Return(sig, &solana.SignatureStatus{
		ErrorResult: solana.NewTransactionError(solana.TransactionErrorAccountNotFound),
	}, nil).Times(2)

	for i := 0; i < 2; i++ {
		resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
			Transaction: &commonpb.Transaction{
				Value: txn.Marshal(),
			},
			Commitment: common.Commitment_MAX,
			DedupeId:   []byte("failed"),
		})
		assert.NoError(t, err)
		assert.Equal(t, transactionpb.SubmitTransactionResponse_FAILED, resp.Result)
		assert.NotNil(t, resp.TransactionError)
		require.NotEmpty(t, sig[:], resp.Signature.Value)
	}

	env.authorizer.AssertExpectations(t)
	env.webhookSubmitter.AssertExpectations(t)
}

func TestSubmitTransaction_DedupeConcurrent(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	txn, accounts := generateTransaction(t, env.subsidizer.Public().(ed25519.PublicKey), 1, 0, nil, nil)

	auth := transaction.Authorization{
		Result: transaction.AuthorizationResultOK,
		SignResponse: &signtransaction.SuccessResponse{
			Signature: ed25519.Sign(env.subsidizer, txn.Message.Marshal()),
		},
	}
	env.authorizer.On("Authorize", mock.Anything, txn, (*commonpbv3.InvoiceList)(nil), true).Return(auth, nil).Times(7)
	env.webhookSubmitter.On("Submit", mock.Anything, mock.Anything).Return(nil).Times(2)
	env.sc.On("GetAccountInfo", mock.Anything, mock.Anything).Return(solana.AccountInfo{}, nil)

	var sig solana.Signature
	copy(sig[:], ed25519.Sign(env.subsidizer, txn.Message.Marshal()))

	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentRoot).Return(sig, &solana.SignatureStatus{}, nil).Times(2)

	for _, a := range accounts {
		info := &accountpb.AccountInfo{
			AccountId: &commonpb.SolanaAccountId{
				Value: a,
			},
			Balance: 10,
		}
		assert.NoError(t, env.infoCache.Put(context.Background(), info))
	}

	assert.NoError(t, env.deduper.Update(context.Background(), []byte("limbo"), &dedupe.Info{
		Signature:      sig[:],
		Response:       nil,
		SubmissionTime: time.Now(),
	}))

	for i := 0; i < 5; i++ {
		resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
			Transaction: &commonpb.Transaction{
				Value: txn.Marshal(),
			},
			Commitment: common.Commitment_ROOT,
			DedupeId:   []byte("limbo"),
		})
		assert.NoError(t, err)
		assert.Equal(t, transactionpb.SubmitTransactionResponse_ALREADY_SUBMITTED, resp.Result)
		require.NotEmpty(t, sig[:], resp.Signature.Value)
	}

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
		Commitment: common.Commitment_ROOT,
		DedupeId:   []byte("unrelated"),
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.SubmitTransactionResponse_OK, resp.Result)
	assert.Nil(t, resp.TransactionError)
	require.NotEmpty(t, sig[:], resp.Signature.Value)

	assert.NoError(t, env.deduper.Delete(context.Background(), []byte("limbo")))

	resp, err = env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
		Commitment: common.Commitment_ROOT,
		DedupeId:   []byte("limbo"),
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.SubmitTransactionResponse_OK, resp.Result)
	assert.Nil(t, resp.TransactionError)
	require.NotEmpty(t, sig[:], resp.Signature.Value)

	env.authorizer.AssertExpectations(t)
	env.webhookSubmitter.AssertExpectations(t)
}

func TestSubmitTransaction_Plain_Batch(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	txn, accounts := generateTransaction(t, env.subsidizer.Public().(ed25519.PublicKey), 3, 0, nil, nil)

	for _, a := range accounts {
		tokenAccountInfo := token.Account{
			Mint:   env.token,
			Owner:  a,
			Amount: 100,
		}
		accountInfo := solana.AccountInfo{
			Owner: token.ProgramKey,
			Data:  tokenAccountInfo.Marshal(),
		}
		env.sc.On("GetAccountInfo", a, mock.Anything).Return(accountInfo, nil)
	}

	auth := transaction.Authorization{
		Result: transaction.AuthorizationResultOK,
		SignResponse: &signtransaction.SuccessResponse{
			Signature: ed25519.Sign(env.subsidizer, txn.Message.Marshal()),
		},
	}
	env.authorizer.On("Authorize", mock.Anything, txn, (*commonpbv3.InvoiceList)(nil), true).Return(auth, nil)
	env.webhookSubmitter.On("Submit", mock.Anything, mock.Anything).Return(nil)

	var sig solana.Signature
	copy(sig[:], ed25519.Sign(env.subsidizer, txn.Message.Marshal()))

	var submitted solana.Transaction
	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentRoot).
		Run(func(args mock.Arguments) {
			submitted = args.Get(0).(solana.Transaction)
		}).
		Return(sig, &solana.SignatureStatus{}, nil)

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
		Commitment: common.Commitment_ROOT,
	})
	assert.NoError(t, err)
	assert.Equal(t, sig[:], resp.Signature.Value)
	assert.Equal(t, submitted.Signatures[0][:], resp.Signature.Value)
	assert.Len(t, env.rw.Writes, 1)

	assert.NoError(t, txn.Sign(env.subsidizer))

	for i, a := range accounts {
		info, err := env.infoCache.Get(context.Background(), a)
		assert.NoError(t, err)

		if i == 0 {
			// Receivers get: 1, 2, and 3. We could use (n/2)(n[0]+n[len(n)]),
			// but we're not actually changing this, and it makes it less clear
			assert.EqualValues(t, 94, info.Balance)
		} else {
			assert.EqualValues(t, 100+i, info.Balance)
		}
	}
}

func TestSubmitTransaction_PartialSpeculativeFailure(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	txn, accounts := generateTransaction(t, env.subsidizer.Public().(ed25519.PublicKey), 3, 0, nil, nil)

	for i, a := range accounts {
		if i == 0 {
			env.sc.On("GetAccountInfo", a, mock.Anything).Return(solana.AccountInfo{}, errors.New("failure"))
			continue
		}

		tokenAccountInfo := token.Account{
			Mint:   env.token,
			Owner:  a,
			Amount: 100,
		}
		accountInfo := solana.AccountInfo{
			Owner: token.ProgramKey,
			Data:  tokenAccountInfo.Marshal(),
		}
		env.sc.On("GetAccountInfo", a, mock.Anything).Return(accountInfo, nil)
	}

	auth := transaction.Authorization{
		Result: transaction.AuthorizationResultOK,
		SignResponse: &signtransaction.SuccessResponse{
			Signature: ed25519.Sign(env.subsidizer, txn.Message.Marshal()),
		},
	}
	env.authorizer.On("Authorize", mock.Anything, txn, (*commonpbv3.InvoiceList)(nil), true).Return(auth, nil)
	env.webhookSubmitter.On("Submit", mock.Anything, mock.Anything).Return(nil)

	var sig solana.Signature
	copy(sig[:], ed25519.Sign(env.subsidizer, txn.Message.Marshal()))

	var submitted solana.Transaction
	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentRoot).
		Run(func(args mock.Arguments) {
			submitted = args.Get(0).(solana.Transaction)
		}).
		Return(sig, &solana.SignatureStatus{}, nil)

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
		Commitment: common.Commitment_ROOT,
	})
	assert.NoError(t, err)
	assert.Equal(t, sig[:], resp.Signature.Value)
	assert.Equal(t, submitted.Signatures[0][:], resp.Signature.Value)
	assert.Len(t, env.rw.Writes, 1)

	assert.NoError(t, txn.Sign(env.subsidizer))

	for i, a := range accounts {
		ai, err := env.infoCache.Get(context.Background(), a)

		if i == 0 {
			assert.Error(t, info.ErrAccountInfoNotFound)
		} else {
			assert.NoError(t, err)
			assert.EqualValues(t, 100+i, ai.Balance)
		}
	}
}

func TestSubmitTransaction_ChainedSpeculativeSubmissions(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	txn, accounts := generateTransaction(t, env.subsidizer.Public().(ed25519.PublicKey), 1, 0, nil, nil)

	auth := transaction.Authorization{
		Result: transaction.AuthorizationResultOK,
		SignResponse: &signtransaction.SuccessResponse{
			Signature: ed25519.Sign(env.subsidizer, txn.Message.Marshal()),
		},
	}
	env.authorizer.On("Authorize", mock.Anything, txn, (*commonpbv3.InvoiceList)(nil), true).Return(auth, nil).Times(3)
	env.webhookSubmitter.On("Submit", mock.Anything, mock.Anything).Return(nil).Times(3)

	senderInfo := token.Account{
		Mint:   env.token,
		Owner:  accounts[0],
		Amount: 10,
		State:  token.AccountStateInitialized,
	}
	receiverInfo := token.Account{
		Mint:   env.token,
		Owner:  accounts[1],
		Amount: 10,
		State:  token.AccountStateInitialized,
	}
	accountInfos := []solana.AccountInfo{
		{
			Owner: token.ProgramKey,
			Data:  senderInfo.Marshal(),
		},
		{
			Owner: token.ProgramKey,
			Data:  receiverInfo.Marshal(),
		},
	}

	env.sc.On("GetAccountInfo", accounts[0], mock.Anything).Return(accountInfos[0], nil)
	env.sc.On("GetAccountInfo", accounts[1], mock.Anything).Return(accountInfos[1], nil)

	var sig solana.Signature
	copy(sig[:], ed25519.Sign(env.subsidizer, txn.Message.Marshal()))
	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentRoot).Return(sig, &solana.SignatureStatus{}, nil)

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
		Commitment: common.Commitment_ROOT,
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.SubmitTransactionResponse_OK, resp.Result)

	// Ensure speculative updates is working
	for i, a := range accounts {
		info, err := env.infoCache.Get(context.Background(), a)
		assert.NoError(t, err)

		if i == 0 {
			assert.EqualValues(t, 9, info.Balance)
		} else {
			assert.EqualValues(t, 11, info.Balance)
		}
	}

	txn.SetBlockhash(solana.Blockhash{})
	resp, err = env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
		Commitment: common.Commitment_ROOT,
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.SubmitTransactionResponse_OK, resp.Result)

	// Ensure speculative updates is working
	for i, a := range accounts {
		info, err := env.infoCache.Get(context.Background(), a)
		assert.NoError(t, err)

		if i == 0 {
			assert.EqualValues(t, 8, info.Balance)
		} else {
			assert.EqualValues(t, 12, info.Balance)
		}
	}

	// To ensure we were loading off cache, we will clear cache and do a submit,
	// which should not reflect the speculative predictions.
	for _, a := range accounts {
		deleted, err := env.infoCache.Del(context.Background(), a)
		assert.NoError(t, err)
		assert.True(t, deleted)
	}

	resp, err = env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
		Commitment: common.Commitment_ROOT,
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.SubmitTransactionResponse_OK, resp.Result)

	for i, a := range accounts {
		info, err := env.infoCache.Get(context.Background(), a)
		assert.NoError(t, err)

		if i == 0 {
			assert.EqualValues(t, 9, info.Balance)
		} else {
			assert.EqualValues(t, 11, info.Balance)
		}
	}

	env.webhookSubmitter.AssertExpectations(t)
}

// All uses of SubmitTransaction use speculative execution, with the expectations
// of complete successes. This will test partial and complete failures.
func TestSubmitTransaction_Invoice(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	invoice, invoiceHash, _ := generateInvoice(t, 1)
	txn, _ := generateTransaction(t, env.subsidizer.Public().(ed25519.PublicKey), 1, 0, invoiceHash, nil)

	auth := transaction.Authorization{
		Result: transaction.AuthorizationResultOK,
		SignResponse: &signtransaction.SuccessResponse{
			Signature: ed25519.Sign(env.subsidizer, txn.Message.Marshal()),
		},
	}

	var calledInvoice *commonpbv3.InvoiceList
	env.authorizer.On("Authorize", mock.Anything, txn, mock.Anything, true).Return(auth, nil).Run(func(args mock.Arguments) {
		calledInvoice = args.Get(2).(*commonpbv3.InvoiceList)
	})
	env.webhookSubmitter.On("Submit", mock.Anything, mock.Anything).Return(nil)
	env.sc.On("GetAccountInfo", mock.Anything, mock.Anything).Return(solana.AccountInfo{}, nil)

	var sig solana.Signature
	copy(sig[:], ed25519.Sign(env.subsidizer, txn.Message.Marshal()))
	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentRoot).Return(sig, &solana.SignatureStatus{}, nil)

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
		InvoiceList: invoice,
		Commitment:  common.Commitment_ROOT,
	})
	assert.NoError(t, err)
	assert.Equal(t, sig[:], resp.Signature.Value)
	assert.Len(t, env.rw.Writes, 1)
	stored, err := env.invoiceStore.Get(context.Background(), sig[:])
	assert.NoError(t, err)
	assert.True(t, proto.Equal(stored, invoice))

	assert.NoError(t, txn.Sign(env.subsidizer))
	assert.True(t, proto.Equal(calledInvoice, invoice))
}

func TestSubmitTransaction_Invoice_Batch(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	invoice, invoiceHash, _ := generateInvoice(t, 3)
	txn, _ := generateTransaction(t, env.subsidizer.Public().(ed25519.PublicKey), 3, 0, invoiceHash, nil)

	auth := transaction.Authorization{
		Result: transaction.AuthorizationResultOK,
		SignResponse: &signtransaction.SuccessResponse{
			Signature: ed25519.Sign(env.subsidizer, txn.Message.Marshal()),
		},
	}

	var calledInvoice *commonpbv3.InvoiceList
	env.authorizer.On("Authorize", mock.Anything, txn, mock.Anything, true).Return(auth, nil).Run(func(args mock.Arguments) {
		calledInvoice = args.Get(2).(*commonpbv3.InvoiceList)
	})
	env.webhookSubmitter.On("Submit", mock.Anything, mock.Anything).Return(nil)

	var sig solana.Signature
	copy(sig[:], ed25519.Sign(env.subsidizer, txn.Message.Marshal()))
	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentRoot).Return(sig, &solana.SignatureStatus{}, nil)
	env.sc.On("GetAccountInfo", mock.Anything, mock.Anything).Return(solana.AccountInfo{}, nil)

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
		InvoiceList: invoice,
		Commitment:  common.Commitment_ROOT,
	})
	assert.NoError(t, err)
	assert.Equal(t, sig[:], resp.Signature.Value)
	assert.Len(t, env.rw.Writes, 1)
	stored, err := env.invoiceStore.Get(context.Background(), sig[:])
	assert.NoError(t, err)
	assert.True(t, proto.Equal(stored, invoice))

	assert.NoError(t, txn.Sign(env.subsidizer))
	assert.True(t, proto.Equal(calledInvoice, invoice))
}

func TestSubmitTransaction_Text_MaybeB64(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	memo := "test"
	txn, _ := generateTransaction(t, env.subsidizer.Public().(ed25519.PublicKey), 1, 0, nil, &memo)

	auth := transaction.Authorization{
		Result: transaction.AuthorizationResultOK,
		SignResponse: &signtransaction.SuccessResponse{
			Signature: ed25519.Sign(env.subsidizer, txn.Message.Marshal()),
		},
	}
	env.authorizer.On("Authorize", mock.Anything, txn, (*commonpbv3.InvoiceList)(nil), true).Return(auth, nil)
	env.webhookSubmitter.On("Submit", mock.Anything, mock.Anything).Return(nil)
	env.sc.On("GetAccountInfo", mock.Anything, mock.Anything).Return(solana.AccountInfo{}, nil)

	var sig solana.Signature
	copy(sig[:], ed25519.Sign(env.subsidizer, txn.Message.Marshal()))

	var submitted solana.Transaction
	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentRoot).
		Run(func(args mock.Arguments) {
			submitted = args.Get(0).(solana.Transaction)
		}).
		Return(sig, &solana.SignatureStatus{}, nil)

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
		Commitment: common.Commitment_ROOT,
	})
	assert.NoError(t, err)
	assert.Equal(t, sig[:], resp.Signature.Value)
	assert.Equal(t, submitted.Signatures[0][:], resp.Signature.Value)
	assert.Len(t, env.rw.Writes, 1)

	assert.NoError(t, txn.Sign(env.subsidizer))
}

func TestSubmitTransaction_Text_NotB64(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	memo := "---test"
	txn, _ := generateTransaction(t, env.subsidizer.Public().(ed25519.PublicKey), 1, 0, nil, &memo)

	auth := transaction.Authorization{
		Result: transaction.AuthorizationResultOK,
		SignResponse: &signtransaction.SuccessResponse{
			Signature: ed25519.Sign(env.subsidizer, txn.Message.Marshal()),
		},
	}
	env.authorizer.On("Authorize", mock.Anything, txn, (*commonpbv3.InvoiceList)(nil), true).Return(auth, nil)
	env.webhookSubmitter.On("Submit", mock.Anything, mock.Anything).Return(nil)
	env.sc.On("GetAccountInfo", mock.Anything, mock.Anything).Return(solana.AccountInfo{}, nil)

	var sig solana.Signature
	copy(sig[:], ed25519.Sign(env.subsidizer, txn.Message.Marshal()))

	var submitted solana.Transaction
	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentRoot).
		Run(func(args mock.Arguments) {
			submitted = args.Get(0).(solana.Transaction)
		}).
		Return(sig, &solana.SignatureStatus{}, nil)

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
		Commitment: common.Commitment_ROOT,
	})
	assert.NoError(t, err)
	assert.Equal(t, sig[:], resp.Signature.Value)
	assert.Equal(t, submitted.Signatures[0][:], resp.Signature.Value)
	assert.Len(t, env.rw.Writes, 1)

	assert.NoError(t, txn.Sign(env.subsidizer))
}

func TestSubmitTransaction_Rejected(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	txn, _ := generateTransaction(t, env.subsidizer.Public().(ed25519.PublicKey), 1, 0, nil, nil)

	auth := transaction.Authorization{
		Result: transaction.AuthorizationResultRejected,
	}
	env.authorizer.On("Authorize", mock.Anything, txn, (*commonpbv3.InvoiceList)(nil), true).Return(auth, nil)

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
		Commitment: common.Commitment_ROOT,
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.SubmitTransactionResponse_REJECTED, resp.Result)
	assert.Empty(t, env.sc.Calls)
}

func TestSubmitTransaction_InvoiceErrors(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	txn, _ := generateTransaction(t, env.subsidizer.Public().(ed25519.PublicKey), 1, 0, nil, nil)

	auth := transaction.Authorization{
		Result: transaction.AuthorizationResultInvoiceError,
		InvoiceErrors: []*commonpbv3.InvoiceError{
			{
				OpIndex: 1,
				Invoice: &commonpbv3.Invoice{
					Items: []*commonpbv3.Invoice_LineItem{
						{
							Title:       "test",
							Description: "desc",
							Amount:      10,
						},
					},
				},
				Reason: commonpbv3.InvoiceError_ALREADY_PAID,
			},
		},
	}
	env.authorizer.On("Authorize", mock.Anything, txn, (*commonpbv3.InvoiceList)(nil), true).Return(auth, nil)

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
		Commitment: common.Commitment_ROOT,
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.SubmitTransactionResponse_INVOICE_ERROR, resp.Result)
	assert.True(t, proto.Equal(auth.InvoiceErrors[0], resp.InvoiceErrors[0]))
	assert.Empty(t, env.sc.Calls)
}

func TestSubmitTransaction_NoPayer(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	env.server.subsidizer = nil

	txn, _ := generateTransaction(t, env.subsidizer.Public().(ed25519.PublicKey), 1, 0, nil, nil)

	auth := transaction.Authorization{
		Result: transaction.AuthorizationResultPayerRequired,
	}
	env.authorizer.On("Authorize", mock.Anything, txn, (*commonpbv3.InvoiceList)(nil), true).Return(auth, nil)

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
		Commitment: common.Commitment_ROOT,
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.SubmitTransactionResponse_PAYER_REQUIRED, resp.Result)
	assert.Empty(t, env.sc.Calls)
}

func TestSubmitTransaction_SubmitError(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	txn, _ := generateTransaction(t, env.subsidizer.Public().(ed25519.PublicKey), 1, 0, nil, nil)

	auth := transaction.Authorization{
		Result: transaction.AuthorizationResultOK,
		SignResponse: &signtransaction.SuccessResponse{
			Signature: ed25519.Sign(env.subsidizer, txn.Message.Marshal()),
		},
	}
	env.authorizer.On("Authorize", mock.Anything, txn, (*commonpbv3.InvoiceList)(nil), true).Return(auth, nil)
	env.sc.On("GetAccountInfo", mock.Anything, mock.Anything).Return(solana.AccountInfo{}, nil)

	var sig solana.Signature
	copy(sig[:], ed25519.Sign(env.subsidizer, txn.Message.Marshal()))

	txErr, err := solana.TransactionErrorFromInstructionError(&solana.InstructionError{
		Index: 0,
		Err:   solana.CustomError(token.ErrorInsufficientFunds),
	})
	require.NoError(t, err)
	status := &solana.SignatureStatus{
		ErrorResult: txErr,
	}

	var submitted solana.Transaction
	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentRoot).
		Run(func(args mock.Arguments) {
			submitted = args.Get(0).(solana.Transaction)
		}).
		Return(sig, status, nil)

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
		Commitment: common.Commitment_ROOT,
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.SubmitTransactionResponse_FAILED, resp.Result)
	assert.Equal(t, sig[:], resp.Signature.Value)
	assert.Equal(t, submitted.Signatures[0][:], resp.Signature.Value)
}

func TestSubmitTransaction_SimulationEvent(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	txn, accounts := generateTransaction(t, env.subsidizer.Public().(ed25519.PublicKey), 1, 0, nil, nil)

	auth := transaction.Authorization{
		Result: transaction.AuthorizationResultOK,
		SignResponse: &signtransaction.SuccessResponse{
			Signature: ed25519.Sign(env.subsidizer, txn.Message.Marshal()),
		},
	}
	env.authorizer.On("Authorize", mock.Anything, txn, (*commonpbv3.InvoiceList)(nil), true).Return(auth, nil)
	env.webhookSubmitter.On("Submit", mock.Anything, mock.Anything).Return(nil).Times(1)

	senderInfo := token.Account{
		Mint:   env.token,
		Owner:  accounts[0],
		Amount: 10,
		State:  token.AccountStateInitialized,
	}
	receiverInfo := token.Account{
		Mint:   env.token,
		Owner:  accounts[1],
		Amount: 10,
		State:  token.AccountStateInitialized,
	}
	accountInfos := []solana.AccountInfo{
		{
			Owner: token.ProgramKey,
			Data:  senderInfo.Marshal(),
		},
		{
			Owner: token.ProgramKey,
			Data:  receiverInfo.Marshal(),
		},
	}

	env.sc.On("GetAccountInfo", accounts[0], mock.Anything).Return(accountInfos[0], nil)
	env.sc.On("GetAccountInfo", accounts[1], mock.Anything).Return(accountInfos[1], nil)

	var sig solana.Signature
	copy(sig[:], ed25519.Sign(env.subsidizer, txn.Message.Marshal()))

	var submitted solana.Transaction
	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentRoot).
		Run(func(args mock.Arguments) {
			submitted = args.Get(0).(solana.Transaction)
		}).
		Return(sig, &solana.SignatureStatus{}, nil)

	var simulationResult *solana.TransactionError
	env.sc.On("SimulateTransaction", mock.Anything).Return(simulationResult, nil)

	for _, a := range accounts {
		info := &accountpb.AccountInfo{
			AccountId: &commonpb.SolanaAccountId{
				Value: a,
			},
			Balance: 10,
		}
		assert.NoError(t, env.infoCache.Put(context.Background(), info))
	}

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
		SendSimulationEvent: true,
		Commitment:          common.Commitment_ROOT,
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.SubmitTransactionResponse_OK, resp.Result)
	assert.Equal(t, sig[:], resp.Signature.Value)
	assert.Equal(t, submitted.Signatures[0][:], resp.Signature.Value)
	assert.Len(t, env.rw.Writes, 1)

	assert.NoError(t, txn.Sign(env.subsidizer))

	// Ensure speculative updates is working
	for i, a := range accounts {
		info, err := env.infoCache.Get(context.Background(), a)
		assert.NoError(t, err)

		if i == 0 {
			assert.EqualValues(t, 9, info.Balance)
		} else {
			assert.EqualValues(t, 11, info.Balance)
		}
	}

	env.webhookSubmitter.AssertExpectations(t)
	submittedEntry := env.webhookSubmitter.Calls[0].Arguments.Get(1).(*model.Entry)
	assert.EqualValues(t, txn.Marshal(), submittedEntry.GetSolana().Transaction)
	require.Equal(t, 2, len(env.events))
	assert.Equal(t, txn.Marshal(), env.events[0].GetSimulationEvent().Transaction)
	assert.Equal(t, txn.Marshal(), env.events[1].GetTransactionEvent().Transaction)
}

func TestSubmitTransaction_SimulationEvent_Error(t *testing.T) {
	env, cleanup := setupServerEnv(t)
	defer cleanup()

	txn, _ := generateTransaction(t, env.subsidizer.Public().(ed25519.PublicKey), 1, 0, nil, nil)

	auth := transaction.Authorization{
		Result: transaction.AuthorizationResultOK,
		SignResponse: &signtransaction.SuccessResponse{
			Signature: ed25519.Sign(env.subsidizer, txn.Message.Marshal()),
		},
	}
	env.authorizer.On("Authorize", mock.Anything, txn, (*commonpbv3.InvoiceList)(nil), true).Return(auth, nil)
	env.sc.On("GetAccountInfo", mock.Anything, mock.Anything).Return(solana.AccountInfo{}, nil)

	var sig solana.Signature
	copy(sig[:], ed25519.Sign(env.subsidizer, txn.Message.Marshal()))

	txErr, err := solana.TransactionErrorFromInstructionError(&solana.InstructionError{
		Index: 0,
		Err:   solana.CustomError(token.ErrorInsufficientFunds),
	})
	require.NoError(t, err)
	status := &solana.SignatureStatus{
		ErrorResult: txErr,
	}

	var submitted solana.Transaction
	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentRoot).
		Run(func(args mock.Arguments) {
			submitted = args.Get(0).(solana.Transaction)
		}).
		Return(sig, status, nil)
	env.sc.On("SimulateTransaction", mock.Anything).Return(txErr, nil)

	resp, err := env.client.SubmitTransaction(context.Background(), &transactionpb.SubmitTransactionRequest{
		Transaction: &commonpb.Transaction{
			Value: txn.Marshal(),
		},
		Commitment:          common.Commitment_ROOT,
		SendSimulationEvent: true,
	})
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.SubmitTransactionResponse_FAILED, resp.Result)
	assert.Equal(t, sig[:], resp.Signature.Value)
	assert.Equal(t, submitted.Signatures[0][:], resp.Signature.Value)

	assert.NoError(t, txn.Sign(env.subsidizer))

	require.Equal(t, 1, len(env.events))
	assert.Equal(t, txn.Marshal(), env.events[0].GetSimulationEvent().Transaction)

	transactionError := &commonpb.TransactionError{}
	assert.NoError(t, proto.Unmarshal(env.events[0].GetSimulationEvent().TransactionError, transactionError))

	assert.Equal(t, commonpb.TransactionError_INSUFFICIENT_FUNDS, transactionError.Reason)
	assert.EqualValues(t, 0, transactionError.InstructionIndex)
	jsonString, err := txErr.JSONString()
	assert.NoError(t, err)
	assert.Equal(t, jsonString, string(transactionError.Raw))
}

func generateTransaction(t *testing.T, subsidizer ed25519.PublicKey, numReceivers, numCloseAccounts int, invoiceHash []byte, textMemo *string) (solana.Transaction, []ed25519.PublicKey) {
	sender := testutil.GenerateSolanaKeypair(t)
	receivers := testutil.GenerateSolanaKeys(t, numReceivers)

	accounts := make([]ed25519.PublicKey, 0, len(sender)+len(receivers))
	accounts = append(accounts, sender.Public().(ed25519.PublicKey))
	accounts = append(accounts, receivers...)

	var instructions []solana.Instruction

	if invoiceHash != nil {
		memo, err := kin.NewMemo(1, kin.TransactionTypeSpend, 1, invoiceHash)
		require.NoError(t, err)
		instructions = append(instructions, solanamemo.Instruction(base64.StdEncoding.EncodeToString(memo[:])))
	} else if textMemo != nil {
		instructions = append(instructions, solanamemo.Instruction(*textMemo))
	}

	for i := range receivers {
		instructions = append(
			instructions,
			token.Transfer(
				sender.Public().(ed25519.PublicKey),
				receivers[i],
				sender.Public().(ed25519.PublicKey),
				uint64(i+1),
			),
		)

		if numCloseAccounts > 0 {
			instructions = append(
				instructions,
				token.CloseAccount(
					receivers[i],
					sender.Public().(ed25519.PublicKey),
					receivers[i],
				),
			)
			numCloseAccounts--
		}
	}

	txn := solana.NewTransaction(
		subsidizer,
		instructions...,
	)
	assert.NoError(t, txn.Sign(sender))

	for i := range txn.Message.Instructions {
		if txn.Message.Instructions[i].Accounts == nil {
			// Note: we do this to satisfy the mock comparators. They're functionally the same,
			// but a nil slice and an empty slice aren't considered the same by mock.
			txn.Message.Instructions[i].Accounts = make([]byte, 0)
		}
	}

	return txn, accounts
}
