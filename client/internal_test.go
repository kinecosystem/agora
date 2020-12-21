package client

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"github.com/kinecosystem/agora-common/headers"
	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/agora-common/solana"
	solanamemo "github.com/kinecosystem/agora-common/solana/memo"
	"github.com/kinecosystem/agora-common/solana/system"
	"github.com/kinecosystem/agora-common/solana/token"
	agoratestutil "github.com/kinecosystem/agora-common/testutil"
	"github.com/kinecosystem/go/keypair"
	"github.com/pkg/errors"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v3"
	accountpbv4 "github.com/kinecosystem/agora-api/genproto/account/v4"
	airdroppbv4 "github.com/kinecosystem/agora-api/genproto/airdrop/v4"
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"
	commonpbv4 "github.com/kinecosystem/agora-api/genproto/common/v4"
	transactionpb "github.com/kinecosystem/agora-api/genproto/transaction/v3"
	transactionpbv4 "github.com/kinecosystem/agora-api/genproto/transaction/v4"

	"github.com/kinecosystem/agora/client/testserver"
	"github.com/kinecosystem/agora/pkg/testutil"
	"github.com/kinecosystem/agora/pkg/version"
)

type testEnv struct {
	server   *testServer
	v4Server *testserver.V4Server
	conn     *grpc.ClientConn
	internal *InternalClient
	client   *client
}

func setup(t *testing.T, opts ...ClientOption) (*testEnv, func()) {
	env := &testEnv{
		server:   newTestServer(),
		v4Server: testserver.NewV4Server(),
	}

	conn, serv, err := agoratestutil.NewServer(
		agoratestutil.WithUnaryServerInterceptor(headers.UnaryServerInterceptor()),
		agoratestutil.WithStreamServerInterceptor(headers.StreamServerInterceptor()),
		agoratestutil.WithUnaryServerInterceptor(version.MinVersionUnaryServerInterceptor()),
	)
	require.NoError(t, err)

	serv.RegisterService(func(s *grpc.Server) {
		accountpb.RegisterAccountServer(s, env.server)
		transactionpb.RegisterTransactionServer(s, env.server)
		accountpbv4.RegisterAccountServer(s, env.v4Server)
		transactionpbv4.RegisterTransactionServer(s, env.v4Server)
		airdroppbv4.RegisterAirdropServer(s, env.v4Server)
	})

	env.conn = conn

	defaultOpts := []ClientOption{
		WithGRPC(conn),
		WithAppIndex(1),
		WithMaxRetries(3),
		WithMinDelay(time.Millisecond),
		WithMaxDelay(time.Millisecond),
	}
	opts = append(defaultOpts, opts...)
	c, err := New(
		EnvironmentTest,
		opts...,
	)
	require.NoError(t, err)

	env.client = c.(*client)
	env.internal = env.client.internal

	cleanup, err := serv.Serve()
	require.NoError(t, err)

	return env, cleanup
}

func TestInternal_BlockchainVersion(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	v, err := env.internal.GetBlockchainVersion(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, version.KinVersion3, v)

	ctx := metadata.AppendToOutgoingContext(context.Background(), "desired-kin-version", "4")
	v, err = env.internal.GetBlockchainVersion(ctx)
	assert.NoError(t, err)
	assert.Equal(t, version.KinVersion4, v)
}

func TestInternal_CreateStellarAccount(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	priv, err := NewPrivateKey()
	require.NoError(t, err)

	accountInfo, err := env.internal.GetStellarAccountInfo(context.Background(), priv.Public())
	assert.Nil(t, accountInfo)
	assert.Equal(t, ErrAccountDoesNotExist, err)

	assert.NoError(t, env.internal.CreateStellarAccount(context.Background(), PrivateKey(priv)))
	assert.Equal(t, ErrAccountExists, env.internal.CreateStellarAccount(context.Background(), PrivateKey(priv)))

	accountInfo, err = env.internal.GetStellarAccountInfo(context.Background(), priv.Public())
	assert.NoError(t, err)
	assert.NotNil(t, accountInfo)
	assert.EqualValues(t, 1, accountInfo.SequenceNumber)
	assert.EqualValues(t, 10, accountInfo.Balance)

	priv, err = NewPrivateKey()
	require.NoError(t, err)
	env.server.setError(errors.New("unexpected"), 2)
	assert.NoError(t, env.internal.CreateStellarAccount(context.Background(), PrivateKey(priv)))

	priv, err = NewPrivateKey()
	require.NoError(t, err)
	env.server.setError(errors.New("unexpected"), 3)
	assert.NotNil(t, env.internal.CreateStellarAccount(context.Background(), PrivateKey(priv)))
}

func TestInternal_CreateStellarAccountKin2(t *testing.T) {
	env, cleanup := setup(t, WithKinVersion(version.KinVersion2))
	defer cleanup()

	priv, err := NewPrivateKey()
	require.NoError(t, err)

	accountInfo, err := env.internal.GetStellarAccountInfo(context.Background(), priv.Public())
	assert.Nil(t, accountInfo)
	assert.Equal(t, ErrAccountDoesNotExist, err)

	assert.NoError(t, env.internal.CreateStellarAccount(context.Background(), PrivateKey(priv)))
	assert.Equal(t, ErrAccountExists, env.internal.CreateStellarAccount(context.Background(), PrivateKey(priv)))

	accountInfo, err = env.internal.GetStellarAccountInfo(context.Background(), priv.Public())
	assert.NoError(t, err)
	assert.NotNil(t, accountInfo)
	assert.EqualValues(t, 1, accountInfo.SequenceNumber)
	assert.EqualValues(t, 10, accountInfo.Balance)

	priv, err = NewPrivateKey()
	require.NoError(t, err)
	env.server.setError(errors.New("unexpected"), 2)
	assert.NoError(t, env.internal.CreateStellarAccount(context.Background(), priv))

	priv, err = NewPrivateKey()
	require.NoError(t, err)
	env.server.setError(errors.New("unexpected"), 3)
	assert.NotNil(t, env.internal.CreateStellarAccount(context.Background(), priv))
}

func TestInternal_GetStellarTransaction(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	_, err := env.internal.GetStellarTransaction(context.Background(), make([]byte, 32))
	assert.Equal(t, ErrTransactionNotFound, err)

	// Test valid combinations of transactions.
	//
	// Any transaction not using the invoice structure may have
	// non-payment types. Therefore, generatePayments() inserts
	// non-payment types into the transaction to ensure the client
	// handles it correctly.
	for _, tc := range []struct {
		sameSource bool
		useInvoice bool
	}{
		{true, false},
		{true, true},
		{false, false},
		{false, true},
	} {
		_, txData, resp := generateV3Payments(t, tc.sameSource, tc.useInvoice, version.KinVersion3)

		env.server.mu.Lock()
		env.server.blockchains[version.KinVersion3].gets[string(txData.TxID)] = resp
		env.server.mu.Unlock()

		actual, err := env.internal.GetStellarTransaction(context.Background(), txData.TxID)
		assert.NoError(t, err)

		assert.EqualValues(t, txData.TxID, actual.TxID)

		// We need to compare fields individually, since EqualValues() fails
		// on proto objects which are semantically the same.
		require.Equal(t, len(txData.Payments), len(actual.Payments))
		for i := 0; i < len(txData.Payments); i++ {
			assert.EqualValues(t, txData.Payments[i].Sender, actual.Payments[i].Sender)
			assert.EqualValues(t, txData.Payments[i].Destination, actual.Payments[i].Destination)
			assert.EqualValues(t, txData.Payments[i].Type, actual.Payments[i].Type)
			assert.EqualValues(t, txData.Payments[i].Quarks, actual.Payments[i].Quarks)
			assert.EqualValues(t, txData.Payments[i].Memo, actual.Payments[i].Memo)

			assert.True(t, proto.Equal(txData.Payments[i].Invoice, actual.Payments[i].Invoice))
		}
	}
}

func TestInternal_GetStellarTransactionKin2(t *testing.T) {
	env, cleanup := setup(t, WithKinVersion(version.KinVersion2))
	defer cleanup()

	_, err := env.internal.GetStellarTransaction(context.Background(), make([]byte, 32))
	assert.Equal(t, ErrTransactionNotFound, err)

	// Test valid combinations of transactions.
	//
	// Any transaction not using the invoice structure may have
	// non-payment types. Therefore, generatePayments() inserts
	// non-payment types into the transaction to ensure the client
	// handles it correctly.
	for _, tc := range []struct {
		sameSource bool
		useInvoice bool
	}{
		{true, false},
		{true, true},
		{false, false},
		{false, true},
	} {
		_, txData, resp := generateV3Payments(t, tc.sameSource, tc.useInvoice, version.KinVersion2)

		env.server.mu.Lock()
		env.server.blockchains[version.KinVersion2].gets[string(txData.TxID)] = resp
		env.server.mu.Unlock()

		actual, err := env.internal.GetStellarTransaction(context.Background(), txData.TxID)
		assert.NoError(t, err)

		assert.EqualValues(t, txData.TxID, actual.TxID)

		// We need to compare fields individually, since EqualValues() fails
		// on proto objects which are semantically the same.
		require.Equal(t, len(txData.Payments), len(actual.Payments))
		for i := 0; i < len(txData.Payments); i++ {
			assert.EqualValues(t, txData.Payments[i].Sender, actual.Payments[i].Sender)
			assert.EqualValues(t, txData.Payments[i].Destination, actual.Payments[i].Destination)
			assert.EqualValues(t, txData.Payments[i].Type, actual.Payments[i].Type)
			assert.EqualValues(t, txData.Payments[i].Quarks, actual.Payments[i].Quarks)
			assert.EqualValues(t, txData.Payments[i].Memo, actual.Payments[i].Memo)

			assert.True(t, proto.Equal(txData.Payments[i].Invoice, actual.Payments[i].Invoice))
		}
	}
}

func TestInternal_SubmitStellarTransaction(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	// Test happy path (hash is returned)
	accounts := testutil.GenerateAccountIDs(t, 2)
	envelope := testutil.GenerateTransactionEnvelope(
		accounts[0],
		1,
		[]xdr.Operation{
			testutil.GeneratePaymentOperation(&accounts[0], accounts[1]),
		},
	)

	envelopeBytes, err := envelope.MarshalBinary()
	require.NoError(t, err)

	txBytes, err := envelope.Tx.MarshalBinary()
	require.NoError(t, err)
	txHash := sha256.Sum256(txBytes)

	txData, err := env.internal.SubmitStellarTransaction(context.Background(), envelopeBytes, nil)
	assert.NoError(t, err)
	assert.EqualValues(t, txHash[:], txData.ID)
	assert.Empty(t, txData.InvoiceErrors)

	invoiceErrors := make([]*commonpb.InvoiceError, 3)
	for i := 0; i < len(invoiceErrors); i++ {
		invoiceErrors[i] = &commonpb.InvoiceError{
			OpIndex: 0,
			Reason:  commonpb.InvoiceError_ALREADY_PAID,
			Invoice: &commonpb.Invoice{
				Items: []*commonpb.Invoice_LineItem{
					{
						Title:  "invoice%d",
						Amount: 0,
					},
				},
			},
		}
	}
	// Test invoice errors propagation
	env.server.mu.Lock()
	env.server.blockchains[version.KinVersion3].submitResponses = []*transactionpb.SubmitTransactionResponse{
		{
			Hash: &commonpb.TransactionHash{
				Value: txHash[:],
			},
			Result:        transactionpb.SubmitTransactionResponse_INVOICE_ERROR,
			InvoiceErrors: invoiceErrors,
		},
	}
	env.server.mu.Unlock()

	txData, err = env.internal.SubmitStellarTransaction(context.Background(), envelopeBytes, nil)
	assert.NoError(t, err)
	assert.EqualValues(t, txHash[:], txData.ID)
	assert.Len(t, txData.InvoiceErrors, len(invoiceErrors))
	for i := 0; i < len(txData.InvoiceErrors); i++ {
		assert.True(t, proto.Equal(txData.InvoiceErrors[i], invoiceErrors[i]))
	}

	// Ensure that the errors field is properly set.
	for _, tc := range []struct {
		handled bool
		code    xdr.TransactionResultCode
	}{
		{
			true,
			xdr.TransactionResultCodeTxBadAuth,
		},
		{
			false,
			xdr.TransactionResultCodeTxTooLate,
		},
	} {
		result := xdr.TransactionResult{
			Result: xdr.TransactionResultResult{
				Code: tc.code,
			},
		}
		resultBytes, err := result.MarshalBinary()
		require.NoError(t, err)

		env.server.mu.Lock()
		env.server.blockchains[version.KinVersion3].submitResponses = []*transactionpb.SubmitTransactionResponse{
			{
				Result: transactionpb.SubmitTransactionResponse_FAILED,
				Hash: &commonpb.TransactionHash{
					Value: txHash[:],
				},
				ResultXdr: resultBytes,
			},
		}
		env.server.mu.Unlock()

		submitResult, err := env.internal.SubmitStellarTransaction(context.Background(), envelopeBytes, nil)
		if tc.handled {
			assert.NoError(t, err)
			assert.EqualValues(t, txHash[:], submitResult.ID)
			assert.Error(t, submitResult.Errors.TxError)
		} else {
			assert.Error(t, err)
			assert.EqualValues(t, txHash[:], submitResult.ID)
		}
	}
}

func TestInternal_SubmitStellarTransactionKin2(t *testing.T) {
	env, cleanup := setup(t, WithKinVersion(version.KinVersion2))
	defer cleanup()

	// Test happy path (hash is returned)
	accounts := testutil.GenerateAccountIDs(t, 2)
	envelope := testutil.GenerateTransactionEnvelope(
		accounts[0],
		1,
		[]xdr.Operation{
			testutil.GeneratePaymentOperation(&accounts[0], accounts[1]),
		},
	)

	envelopeBytes, err := envelope.MarshalBinary()
	require.NoError(t, err)

	txBytes, err := envelope.Tx.MarshalBinary()
	require.NoError(t, err)
	txHash := sha256.Sum256(txBytes)

	txData, err := env.internal.SubmitStellarTransaction(context.Background(), envelopeBytes, nil)
	assert.NoError(t, err)
	assert.EqualValues(t, txHash[:], txData.ID)
	assert.Empty(t, txData.InvoiceErrors)

	invoiceErrors := make([]*commonpb.InvoiceError, 3)
	for i := 0; i < len(invoiceErrors); i++ {
		invoiceErrors[i] = &commonpb.InvoiceError{
			OpIndex: 0,
			Reason:  commonpb.InvoiceError_ALREADY_PAID,
			Invoice: &commonpb.Invoice{
				Items: []*commonpb.Invoice_LineItem{
					{
						Title:  "invoice%d",
						Amount: 0,
					},
				},
			},
		}
	}
	// Test invoice errors propagation
	env.server.mu.Lock()
	env.server.blockchains[version.KinVersion2].submitResponses = []*transactionpb.SubmitTransactionResponse{
		{
			Hash: &commonpb.TransactionHash{
				Value: txHash[:],
			},
			Result:        transactionpb.SubmitTransactionResponse_INVOICE_ERROR,
			InvoiceErrors: invoiceErrors,
		},
	}
	env.server.mu.Unlock()

	txData, err = env.internal.SubmitStellarTransaction(context.Background(), envelopeBytes, nil)
	assert.NoError(t, err)
	assert.EqualValues(t, txHash[:], txData.ID)
	assert.Len(t, txData.InvoiceErrors, len(invoiceErrors))
	for i := 0; i < len(txData.InvoiceErrors); i++ {
		assert.True(t, proto.Equal(txData.InvoiceErrors[i], invoiceErrors[i]))
	}

	// Ensure that the errors field is properly set.
	for _, tc := range []struct {
		handled bool
		code    xdr.TransactionResultCode
	}{
		{
			true,
			xdr.TransactionResultCodeTxBadAuth,
		},
		{
			false,
			xdr.TransactionResultCodeTxTooLate,
		},
	} {
		result := xdr.TransactionResult{
			Result: xdr.TransactionResultResult{
				Code: tc.code,
			},
		}
		resultBytes, err := result.MarshalBinary()
		require.NoError(t, err)

		env.server.mu.Lock()
		env.server.blockchains[version.KinVersion2].submitResponses = []*transactionpb.SubmitTransactionResponse{
			{
				Result: transactionpb.SubmitTransactionResponse_FAILED,
				Hash: &commonpb.TransactionHash{
					Value: txHash[:],
				},
				ResultXdr: resultBytes,
			},
		}
		env.server.mu.Unlock()

		submitResult, err := env.internal.SubmitStellarTransaction(context.Background(), envelopeBytes, nil)
		if tc.handled {
			assert.NoError(t, err)
			assert.EqualValues(t, txHash[:], submitResult.ID)
			assert.Error(t, submitResult.Errors.TxError)
		} else {
			assert.Error(t, err)
			assert.EqualValues(t, txHash[:], submitResult.ID)
		}
	}
}

func TestInternal_SolanaAccountRoundTrip(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	tokenKey, tokenProgram, subsidizer := setServiceConfigResp(t, env.v4Server, true)

	priv, err := NewPrivateKey()
	require.NoError(t, err)
	tokenAcc, _ := generateTokenAccount(ed25519.PrivateKey(priv))

	accountInfo, err := env.internal.GetSolanaAccountInfo(context.Background(), PublicKey(tokenAcc), commonpbv4.Commitment_SINGLE)
	assert.Nil(t, accountInfo)
	assert.Equal(t, ErrAccountDoesNotExist, err)

	assert.NoError(t, env.internal.CreateSolanaAccount(context.Background(), priv, commonpbv4.Commitment_SINGLE, nil))
	assert.Equal(t, ErrAccountExists, env.internal.CreateSolanaAccount(context.Background(), priv, commonpbv4.Commitment_SINGLE, nil))

	accountInfo, err = env.internal.GetSolanaAccountInfo(context.Background(), PublicKey(tokenAcc), commonpbv4.Commitment_SINGLE)
	assert.NoError(t, err)
	assert.NotNil(t, accountInfo)
	assert.EqualValues(t, 10, accountInfo.Balance)

	env.v4Server.Mux.Lock()
	assert.Len(t, env.v4Server.Creates, 2)
	assert.True(t, proto.Equal(env.v4Server.Creates[0], env.v4Server.Creates[1]))
	createReq := env.v4Server.Creates[0]
	env.v4Server.Mux.Unlock()

	tx := solana.Transaction{}
	require.NoError(t, tx.Unmarshal(createReq.Transaction.Value))
	assert.Len(t, tx.Signatures, 3)
	assert.True(t, ed25519.Verify(tokenAcc, tx.Message.Marshal(), tx.Signatures[1][:]))
	assert.True(t, ed25519.Verify(ed25519.PublicKey(priv.Public()), tx.Message.Marshal(), tx.Signatures[2][:]))

	sysCreate, err := system.DecompileCreateAccount(tx.Message, 0)
	require.NoError(t, err)
	assert.Equal(t, subsidizer, sysCreate.Funder)
	assert.EqualValues(t, tokenAcc, sysCreate.Address)
	assert.Equal(t, tokenProgram, sysCreate.Owner)
	assert.Equal(t, testserver.MinBalanceForRentException, sysCreate.Lamports)
	assert.Equal(t, token.AccountSize, int(sysCreate.Size))

	tokenInit, err := token.DecompileInitializeAccount(tx.Message, 1)
	require.NoError(t, err)
	assert.EqualValues(t, tokenAcc, tokenInit.Account)
	assert.Equal(t, tokenKey, tokenInit.Mint)
	assert.EqualValues(t, priv.Public(), tokenInit.Owner)

	setAuth, err := token.DecompileSetAuthority(tx.Message, 2)
	require.NoError(t, err)
	assert.EqualValues(t, tokenAcc, setAuth.Account)
	assert.EqualValues(t, priv.Public(), setAuth.CurrentAuthority)
	assert.Equal(t, subsidizer, setAuth.NewAuthority)
	assert.Equal(t, token.AuthorityTypeCloseAccount, setAuth.Type)
}

func TestInternal_CreateNoServiceSubsidizer(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	tokenKey, tokenProgram, _ := setServiceConfigResp(t, env.v4Server, false)

	priv, err := NewPrivateKey()
	require.NoError(t, err)
	tokenAcc, _ := generateTokenAccount(ed25519.PrivateKey(priv))

	err = env.internal.CreateSolanaAccount(context.Background(), priv, commonpbv4.Commitment_SINGLE, nil)
	require.Equal(t, ErrNoSubsidizer, err)

	subsidizer, err := NewPrivateKey()
	require.NoError(t, err)

	assert.NoError(t, env.internal.CreateSolanaAccount(context.Background(), priv, commonpbv4.Commitment_SINGLE, subsidizer))

	env.v4Server.Mux.Lock()
	assert.Len(t, env.v4Server.Creates, 1)
	createReq := env.v4Server.Creates[0] // validate subsidized create request
	env.v4Server.Mux.Unlock()

	tx := solana.Transaction{}
	require.NoError(t, tx.Unmarshal(createReq.Transaction.Value))
	assert.Len(t, tx.Signatures, 3)
	assert.True(t, ed25519.Verify(ed25519.PublicKey(subsidizer.Public()), tx.Message.Marshal(), tx.Signatures[0][:]))
	assert.True(t, ed25519.Verify(tokenAcc, tx.Message.Marshal(), tx.Signatures[1][:]))
	assert.True(t, ed25519.Verify(ed25519.PublicKey(priv.Public()), tx.Message.Marshal(), tx.Signatures[2][:]))

	sysCreate, err := system.DecompileCreateAccount(tx.Message, 0)
	require.NoError(t, err)
	assert.EqualValues(t, subsidizer.Public(), sysCreate.Funder)
	assert.EqualValues(t, tokenAcc, sysCreate.Address)
	assert.Equal(t, tokenProgram, sysCreate.Owner)
	assert.Equal(t, testserver.MinBalanceForRentException, sysCreate.Lamports)
	assert.Equal(t, token.AccountSize, int(sysCreate.Size))

	tokenInit, err := token.DecompileInitializeAccount(tx.Message, 1)
	require.NoError(t, err)
	assert.EqualValues(t, tokenAcc, tokenInit.Account)
	assert.Equal(t, tokenKey, tokenInit.Mint)
	assert.EqualValues(t, priv.Public(), tokenInit.Owner)

	setAuth, err := token.DecompileSetAuthority(tx.Message, 2)
	require.NoError(t, err)
	assert.EqualValues(t, tokenAcc, setAuth.Account)
	assert.EqualValues(t, priv.Public(), setAuth.CurrentAuthority)
	assert.EqualValues(t, subsidizer.Public(), setAuth.NewAuthority)
	assert.Equal(t, token.AuthorityTypeCloseAccount, setAuth.Type)
}

func TestInternal_GetTransaction(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	txData, err := env.internal.GetTransaction(context.Background(), make([]byte, 32), commonpbv4.Commitment_SINGLE)
	require.NoError(t, err)
	assert.Equal(t, TransactionStateUnknown, txData.TxState)

	// Test valid combinations of transactions.
	//
	// Any transaction not using the invoice structure may have
	// non-payment types. Therefore, generatePayments() inserts
	// non-payment types into the transaction to ensure the client
	// handles it correctly.
	for _, tc := range []struct {
		sameSource bool
		useInvoice bool
	}{
		{true, false},
		{true, true},
		{false, false},
		{false, true},
	} {
		_, txData, resp := generateV4StellarPayments(t, tc.sameSource, tc.useInvoice, version.KinVersion3)

		env.v4Server.Mux.Lock()
		env.v4Server.Gets[string(txData.TxID)] = resp
		env.v4Server.Mux.Unlock()

		actual, err := env.internal.GetTransaction(context.Background(), txData.TxID, commonpbv4.Commitment_SINGLE)
		assert.NoError(t, err)

		assert.EqualValues(t, txData.TxID, actual.TxID)

		// We need to compare fields individually, since EqualValues() fails
		// on proto objects which are semantically the same.
		require.Equal(t, len(txData.Payments), len(actual.Payments))
		for i := 0; i < len(txData.Payments); i++ {
			assert.EqualValues(t, txData.Payments[i].Sender, actual.Payments[i].Sender)
			assert.EqualValues(t, txData.Payments[i].Destination, actual.Payments[i].Destination)
			assert.EqualValues(t, txData.Payments[i].Type, actual.Payments[i].Type)
			assert.EqualValues(t, txData.Payments[i].Quarks, actual.Payments[i].Quarks)
			assert.EqualValues(t, txData.Payments[i].Memo, actual.Payments[i].Memo)

			assert.True(t, proto.Equal(txData.Payments[i].Invoice, actual.Payments[i].Invoice))
		}
	}
}

func TestInternal_GetTransactionKin2(t *testing.T) {
	env, cleanup := setup(t, WithKinVersion(version.KinVersion2))
	defer cleanup()

	txData, err := env.internal.GetTransaction(context.Background(), make([]byte, 32), commonpbv4.Commitment_SINGLE)
	require.NoError(t, err)
	assert.Equal(t, TransactionStateUnknown, txData.TxState)

	// Test valid combinations of transactions.
	//
	// Any transaction not using the invoice structure may have
	// non-payment types. Therefore, generatePayments() inserts
	// non-payment types into the transaction to ensure the client
	// handles it correctly.
	for _, tc := range []struct {
		sameSource bool
		useInvoice bool
	}{
		{true, false},
		{true, true},
		{false, false},
		{false, true},
	} {
		_, txData, resp := generateV4StellarPayments(t, tc.sameSource, tc.useInvoice, version.KinVersion2)

		env.v4Server.Mux.Lock()
		env.v4Server.Gets[string(txData.TxID)] = resp
		env.v4Server.Mux.Unlock()

		actual, err := env.internal.GetTransaction(context.Background(), txData.TxID, commonpbv4.Commitment_SINGLE)
		assert.NoError(t, err)

		assert.EqualValues(t, txData.TxID, actual.TxID)

		// We need to compare fields individually, since EqualValues() fails
		// on proto objects which are semantically the same.
		require.Equal(t, len(txData.Payments), len(actual.Payments))
		for i := 0; i < len(txData.Payments); i++ {
			assert.EqualValues(t, txData.Payments[i].Sender, actual.Payments[i].Sender)
			assert.EqualValues(t, txData.Payments[i].Destination, actual.Payments[i].Destination)
			assert.EqualValues(t, txData.Payments[i].Type, actual.Payments[i].Type)
			assert.EqualValues(t, txData.Payments[i].Quarks, actual.Payments[i].Quarks)
			assert.EqualValues(t, txData.Payments[i].Memo, actual.Payments[i].Memo)

			assert.True(t, proto.Equal(txData.Payments[i].Invoice, actual.Payments[i].Invoice))
		}
	}
}

func TestInternal_GetTransactionKin4(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	txData, err := env.internal.GetTransaction(context.Background(), make([]byte, 32), commonpbv4.Commitment_SINGLE)
	require.NoError(t, err)
	assert.Equal(t, make([]byte, 32), txData.TxID)
	assert.Equal(t, TransactionStateUnknown, txData.TxState)

	for _, tc := range []struct {
		useInvoice bool
	}{
		{false},
		{true},
	} {
		_, txData, resp := generateV4SolanaPayments(t, tc.useInvoice)

		env.v4Server.Mux.Lock()
		env.v4Server.Gets[string(txData.TxID)] = resp
		env.v4Server.Mux.Unlock()

		actual, err := env.internal.GetTransaction(context.Background(), txData.TxID, commonpbv4.Commitment_SINGLE)
		assert.NoError(t, err)

		assert.Equal(t, txData.TxID, actual.TxID)

		// We need to compare fields individually, since EqualValues() fails
		// on proto objects which are semantically the same.
		require.Equal(t, len(txData.Payments), len(actual.Payments))
		for i := 0; i < len(txData.Payments); i++ {
			assert.EqualValues(t, txData.Payments[i].Sender, actual.Payments[i].Sender)
			assert.EqualValues(t, txData.Payments[i].Destination, actual.Payments[i].Destination)
			assert.EqualValues(t, txData.Payments[i].Type, actual.Payments[i].Type)
			assert.EqualValues(t, txData.Payments[i].Quarks, actual.Payments[i].Quarks)
			assert.EqualValues(t, txData.Payments[i].Memo, actual.Payments[i].Memo)

			assert.True(t, proto.Equal(txData.Payments[i].Invoice, actual.Payments[i].Invoice))
		}
	}
}

func TestInternal_GetTransactionWithError(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	txData, err := env.internal.GetTransaction(context.Background(), make([]byte, 32), commonpbv4.Commitment_SINGLE)
	require.NoError(t, err)
	assert.Equal(t, make([]byte, 32), txData.TxID)
	assert.Equal(t, TransactionStateUnknown, txData.TxState)

	_, txData, resp := generateV4SolanaPayments(t, false)
	resp.Item.TransactionError = &commonpbv4.TransactionError{
		Reason: commonpbv4.TransactionError_BAD_NONCE,
		Raw:    []byte("rawerror"),
	}

	env.v4Server.Mux.Lock()
	env.v4Server.Gets[string(txData.TxID)] = resp
	env.v4Server.Mux.Unlock()

	actual, err := env.internal.GetTransaction(context.Background(), txData.TxID, commonpbv4.Commitment_SINGLE)
	assert.NoError(t, err)

	assert.Equal(t, txData.TxID, actual.TxID)

	// We need to compare fields individually, since EqualValues() fails
	// on proto objects which are semantically the same.
	require.Equal(t, len(txData.Payments), len(actual.Payments))
	for i := 0; i < len(txData.Payments); i++ {
		assert.EqualValues(t, txData.Payments[i].Sender, actual.Payments[i].Sender)
		assert.EqualValues(t, txData.Payments[i].Destination, actual.Payments[i].Destination)
		assert.EqualValues(t, txData.Payments[i].Type, actual.Payments[i].Type)
		assert.EqualValues(t, txData.Payments[i].Quarks, actual.Payments[i].Quarks)
		assert.EqualValues(t, txData.Payments[i].Memo, actual.Payments[i].Memo)
		assert.Nil(t, actual.Payments[i].Invoice)
	}

	// Assert the error
	assert.Equal(t, ErrBadNonce, actual.Errors.TxError)
}

func TestInternal_SubmitSolanaTransaction(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	// Test happy path (hash is returned)
	sender, senderKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	dest, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	tx := solana.NewTransaction(
		sender,
		token.Transfer(sender, dest, sender, 10),
	)
	require.NoError(t, tx.Sign(senderKey))

	txSig := tx.Signature()

	randId := uuid.New()
	dedupeId := randId[:]
	il := &commonpb.InvoiceList{
		Invoices: []*commonpb.Invoice{
			{
				Items: []*commonpb.Invoice_LineItem{
					{
						Title: "hi",
					},
				},
			},
		},
	}

	result, err := env.internal.SubmitSolanaTransaction(context.Background(), tx, il, commonpbv4.Commitment_SINGLE, dedupeId)
	require.NoError(t, err)
	assert.EqualValues(t, txSig, result.ID)
	assert.Empty(t, result.InvoiceErrors)

	// Verify server received what was expected
	env.v4Server.Mux.Lock()
	assert.Equal(t, 1, len(env.v4Server.Submits))
	req := env.v4Server.Submits[0]
	assert.Equal(t, tx.Marshal(), req.Transaction.Value)
	assert.True(t, proto.Equal(il, req.InvoiceList))
	assert.Equal(t, commonpbv4.Commitment_SINGLE, req.Commitment)
	assert.Equal(t, dedupeId, req.DedupeId)

	env.v4Server.Mux.Unlock()

	// Test already submitted received on first attempt
	env.v4Server.Mux.Lock()
	env.v4Server.SubmitResponses = []*transactionpbv4.SubmitTransactionResponse{
		{
			Signature: &commonpbv4.TransactionSignature{
				Value: txSig[:],
			},
			Result: transactionpbv4.SubmitTransactionResponse_ALREADY_SUBMITTED,
		},
	}
	env.v4Server.Mux.Unlock()

	result, err = env.internal.SubmitSolanaTransaction(context.Background(), tx, nil, commonpbv4.Commitment_SINGLE, nil)
	assert.Equal(t, ErrAlreadySubmitted, err)

	// Test already submitted received on second attempt
	env.v4Server.SetError(errors.New("unexpected"), 1)
	env.v4Server.Mux.Lock()
	env.v4Server.SubmitResponses = []*transactionpbv4.SubmitTransactionResponse{
		{
			Signature: &commonpbv4.TransactionSignature{
				Value: txSig[:],
			},
			Result: transactionpbv4.SubmitTransactionResponse_ALREADY_SUBMITTED,
		},
	}
	env.v4Server.Mux.Unlock()

	result, err = env.internal.SubmitSolanaTransaction(context.Background(), tx, nil, commonpbv4.Commitment_SINGLE, nil)
	require.NoError(t, err)
	assert.EqualValues(t, txSig, result.ID)
	assert.Empty(t, result.InvoiceErrors)

	invoiceErrors := make([]*commonpb.InvoiceError, 3)
	for i := 0; i < len(invoiceErrors); i++ {
		invoiceErrors[i] = &commonpb.InvoiceError{
			OpIndex: 0,
			Reason:  commonpb.InvoiceError_ALREADY_PAID,
			Invoice: &commonpb.Invoice{
				Items: []*commonpb.Invoice_LineItem{
					{
						Title:  "invoice%d",
						Amount: 0,
					},
				},
			},
		}
	}
	// Test invoice errors propagation
	env.v4Server.Mux.Lock()
	env.v4Server.SubmitResponses = []*transactionpbv4.SubmitTransactionResponse{
		{
			Signature: &commonpbv4.TransactionSignature{
				Value: txSig[:],
			},
			Result:        transactionpbv4.SubmitTransactionResponse_INVOICE_ERROR,
			InvoiceErrors: invoiceErrors,
		},
	}
	env.v4Server.Mux.Unlock()

	result, err = env.internal.SubmitSolanaTransaction(context.Background(), tx, nil, commonpbv4.Commitment_SINGLE, nil)
	assert.NoError(t, err)
	assert.EqualValues(t, txSig[:], result.ID)
	assert.Len(t, result.InvoiceErrors, len(invoiceErrors))
	for i := 0; i < len(result.InvoiceErrors); i++ {
		assert.True(t, proto.Equal(result.InvoiceErrors[i], invoiceErrors[i]))
	}

	// Test error propagation
	env.v4Server.Mux.Lock()
	env.v4Server.SubmitResponses = []*transactionpbv4.SubmitTransactionResponse{
		{
			Signature: &commonpbv4.TransactionSignature{
				Value: txSig[:],
			},
			Result: transactionpbv4.SubmitTransactionResponse_FAILED,
			TransactionError: &commonpbv4.TransactionError{
				Reason: commonpbv4.TransactionError_UNAUTHORIZED,
				Raw:    []byte("rawerror"),
			},
		},
	}
	env.v4Server.Mux.Unlock()

	result, err = env.internal.SubmitSolanaTransaction(context.Background(), tx, nil, commonpbv4.Commitment_SINGLE, nil)
	assert.NoError(t, err)
	assert.EqualValues(t, txSig[:], result.ID)
	assert.Equal(t, ErrInvalidSignature, result.Errors.TxError)
	assert.Empty(t, result.InvoiceErrors)

	// Test raised exceptions
	for _, tc := range []struct {
		result transactionpbv4.SubmitTransactionResponse_Result
		err    error
	}{
		{
			result: transactionpbv4.SubmitTransactionResponse_REJECTED,
			err:    ErrTransactionRejected,
		},
		{
			result: transactionpbv4.SubmitTransactionResponse_PAYER_REQUIRED,
			err:    ErrPayerRequired,
		},
	} {
		env.v4Server.Mux.Lock()
		env.v4Server.SubmitResponses = []*transactionpbv4.SubmitTransactionResponse{
			{
				Signature: &commonpbv4.TransactionSignature{
					Value: txSig[:],
				},
				Result: tc.result,
			},
		}
		env.v4Server.Mux.Unlock()

		result, err = env.internal.SubmitSolanaTransaction(context.Background(), tx, nil, commonpbv4.Commitment_SINGLE, nil)
		assert.Equal(t, tc.err, err)
	}
}

func TestInternal_GetServiceConfigCache(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	tokenKey, tokenProgram, subsidizer := setServiceConfigResp(t, env.v4Server, true)

	config, err := env.internal.GetServiceConfig(context.Background())
	require.NoError(t, err)
	assert.EqualValues(t, tokenKey, config.Token.Value)
	assert.EqualValues(t, tokenProgram, config.TokenProgram.Value)
	assert.EqualValues(t, subsidizer, config.SubsidizerAccount.Value)

	config, err = env.internal.GetServiceConfig(context.Background())
	require.NoError(t, err)
	assert.EqualValues(t, tokenKey, config.Token.Value)
	assert.EqualValues(t, tokenProgram, config.TokenProgram.Value)
	assert.EqualValues(t, subsidizer, config.SubsidizerAccount.Value)

	env.v4Server.Mux.Lock()
	assert.Len(t, env.v4Server.ServiceConfigReqs, 1)
	env.v4Server.Mux.Unlock()
}

func TestInternal_GetRecentBlockhash(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	blockhash, err := env.internal.GetRecentBlockhash(context.Background())
	require.NoError(t, err)
	assert.EqualValues(t, testserver.RecentBlockhash, blockhash[:])
}

func TestInternal_GetMinimumBalanceForRentException(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	balance, err := env.internal.GetMinimumBalanceForRentException(context.Background(), token.AccountSize)
	require.NoError(t, err)
	assert.Equal(t, testserver.MinBalanceForRentException, balance)
}

func TestInternal_RequestAirdrop(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	priv, err := NewPrivateKey()
	require.NoError(t, err)
	tokenAcc, _ := generateTokenAccount(ed25519.PrivateKey(priv))

	// Account doesn't exist
	txID, err := env.internal.RequestAirdrop(context.Background(), PublicKey(tokenAcc), 10, commonpbv4.Commitment_SINGLE)
	assert.Equal(t, ErrAccountDoesNotExist, err)
	assert.Nil(t, txID)

	setServiceConfigResp(t, env.v4Server, true)
	require.NoError(t, env.internal.CreateSolanaAccount(context.Background(), priv, commonpbv4.Commitment_SINGLE, nil))

	// Too much money
	txID, err = env.internal.RequestAirdrop(context.Background(), PublicKey(tokenAcc), testserver.MaxAirdrop+1, commonpbv4.Commitment_SINGLE)
	assert.Equal(t, ErrInsufficientBalance, err)
	assert.Nil(t, txID)

	txID, err = env.internal.RequestAirdrop(context.Background(), PublicKey(tokenAcc), testserver.MaxAirdrop, commonpbv4.Commitment_SINGLE)
	require.NoError(t, err)
	assert.NotNil(t, txID)
}

func TestInternal_ResolveTokenAccounts(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	sender, err := NewPrivateKey()
	require.NoError(t, err)

	accounts, err := env.internal.ResolveTokenAccounts(context.Background(), sender.Public())
	require.NoError(t, err)
	assert.Empty(t, accounts)

	tokenAccount1, err := NewPrivateKey()
	require.NoError(t, err)
	tokenAccount2, err := NewPrivateKey()
	require.NoError(t, err)

	env.v4Server.Mux.Lock()
	env.v4Server.TokenAccounts[sender.Public().Base58()] = []*commonpbv4.SolanaAccountId{
		{
			Value: tokenAccount1.Public(),
		},
		{
			Value: tokenAccount2.Public(),
		},
	}
	env.v4Server.Mux.Unlock()

	accounts, err = env.internal.ResolveTokenAccounts(context.Background(), sender.Public())
	require.NoError(t, err)
	assert.Len(t, accounts, 2)
	assert.Equal(t, tokenAccount1.Public(), accounts[0])
	assert.Equal(t, tokenAccount2.Public(), accounts[1])
}

func setServiceConfigResp(t *testing.T, server *testserver.V4Server, includeSubsidizer bool) (token, tokenProgram, subsidizer ed25519.PublicKey) {
	var err error
	token, _, err = ed25519.GenerateKey(nil)
	require.NoError(t, err)
	tokenProgram, _, err = ed25519.GenerateKey(nil)
	require.NoError(t, err)

	config := &transactionpbv4.GetServiceConfigResponse{
		TokenProgram: &commonpbv4.SolanaAccountId{Value: tokenProgram},
		Token:        &commonpbv4.SolanaAccountId{Value: token},
	}

	var subsidizerKey ed25519.PrivateKey
	if includeSubsidizer {
		subsidizer, subsidizerKey, err = ed25519.GenerateKey(nil)
		require.NoError(t, err)
		config.SubsidizerAccount = &commonpbv4.SolanaAccountId{Value: subsidizer}
	}

	server.Mux.Lock()
	server.Subsidizer = subsidizerKey
	server.ServiceConfig = config
	server.Mux.Unlock()

	return token, tokenProgram, subsidizer
}

func generateV4SolanaPayments(t *testing.T, useInvoice bool) ([]Payment, TransactionData, transactionpbv4.GetTransactionResponse) {
	resp := transactionpbv4.GetTransactionResponse{
		State: transactionpbv4.GetTransactionResponse_SUCCESS,
		Item:  &transactionpbv4.HistoryItem{},
	}

	memoStr := "1-test"
	sender, senderKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	destinations := make([]ed25519.PublicKey, 5)
	for i := 0; i < 5; i++ {
		destinations[i], _, err = ed25519.GenerateKey(nil)
		require.NoError(t, err)
	}

	var invoiceList *commonpb.InvoiceList
	instructions := make([]solana.Instruction, 6)
	if useInvoice {
		var hash []byte
		hash, invoiceList = generateInvoiceList(t, 5)
		memo, err := kin.NewMemo(1, kin.TransactionTypeSpend, 1, hash[:])
		require.NoError(t, err)

		instructions[0] = solanamemo.Instruction(base64.StdEncoding.EncodeToString(memo[:]))

		resp.Item.InvoiceList = invoiceList
	} else {
		instructions[0] = solanamemo.Instruction(memoStr)
	}

	for i := 0; i < 5; i++ {
		instructions[i+1] = token.Transfer(sender, destinations[i], sender, uint64(i+1))
	}

	tx := solana.NewTransaction(sender, instructions...)
	require.NoError(t, tx.Sign(senderKey))
	sig := tx.Signature()

	resp.Item.TransactionId = &commonpbv4.TransactionId{Value: sig}
	rawTx := &transactionpbv4.HistoryItem_SolanaTransaction{
		SolanaTransaction: &commonpbv4.Transaction{},
	}

	rawTx.SolanaTransaction.Value = tx.Marshal()
	resp.Item.RawTransaction = rawTx

	payments := make([]Payment, 5)
	readOnlyPayments := make([]ReadOnlyPayment, 5)
	resp.Item.Payments = make([]*transactionpbv4.HistoryItem_Payment, 5)
	for i := 0; i < 5; i++ {
		payments[i] = Payment{
			Sender:      PrivateKey(senderKey),
			Destination: PublicKey(destinations[i]),
			Quarks:      int64(i + 1),
		}
		if useInvoice {
			payments[i].Invoice = invoiceList.Invoices[i]
			payments[i].Type = kin.TransactionTypeSpend
		} else {
			payments[i].Memo = memoStr
		}

		readOnlyPayments[i] = ReadOnlyPayment{
			Sender:      payments[i].Sender.Public(),
			Destination: payments[i].Destination,
			Type:        payments[i].Type,
			Quarks:      payments[i].Quarks,
			Invoice:     payments[i].Invoice,
			Memo:        payments[i].Memo,
		}

		resp.Item.Payments[i] = &transactionpbv4.HistoryItem_Payment{
			Source:      &commonpbv4.SolanaAccountId{Value: sender},
			Destination: &commonpbv4.SolanaAccountId{Value: destinations[i]},
			Amount:      int64(i + 1),
			Index:       uint32(i),
		}
	}

	return payments, TransactionData{TxID: sig[:], Payments: readOnlyPayments}, resp
}

func generateV4StellarPayments(t *testing.T, sameSource, useInvoice bool, kinVersion version.KinVersion) ([]Payment, TransactionData, transactionpbv4.GetTransactionResponse) {
	memoStr := "1-test"
	sender, senderAccount := testutil.GenerateAccountID(t)
	senderKey, err := PrivateKeyFromString(sender.Seed())
	require.NoError(t, err)

	var txSourceKey PrivateKey
	var txSourceAccount xdr.AccountId
	if sameSource {
		txSourceAccount = senderAccount
	} else {
		var txSource *keypair.Full
		txSource, txSourceAccount = testutil.GenerateAccountID(t)
		txSourceKey, err = PrivateKeyFromString(txSource.Seed())
		require.NoError(t, err)
	}

	receivers := testutil.GenerateAccountIDs(t, 6)
	ops := make([]xdr.Operation, 0)
	if !useInvoice {
		ops = append(ops, testutil.GenerateCreateOperation(&senderAccount, receivers[0]))
	}
	for i := 0; i < 5; i++ {
		if kinVersion == 2 {
			issuer, err := testutil.StellarAccountIDFromString(kin.Kin2TestIssuer)
			require.NoError(t, err)

			ops = append(ops, testutil.GenerateKin2PaymentOperation(&senderAccount, receivers[i+1], issuer))
		} else {
			ops = append(ops, testutil.GeneratePaymentOperation(&senderAccount, receivers[i+1]))
		}
	}

	envelope := testutil.GenerateTransactionEnvelope(txSourceAccount, 1, ops)
	resp := transactionpbv4.GetTransactionResponse{
		State: transactionpbv4.GetTransactionResponse_SUCCESS,
		Item:  &transactionpbv4.HistoryItem{},
	}

	var invoiceList *commonpb.InvoiceList
	if useInvoice {
		var hash []byte
		hash, invoiceList = generateInvoiceList(t, 5)
		memo, err := kin.NewMemo(1, kin.TransactionTypeSpend, 1, hash[:])
		require.NoError(t, err)

		envelope.Tx.Memo = xdr.Memo{
			Type: xdr.MemoTypeMemoHash,
			Hash: (*xdr.Hash)(&memo),
		}
		resp.Item.InvoiceList = invoiceList
	} else {
		envelope.Tx.Memo = xdr.Memo{
			Type: xdr.MemoTypeMemoText,
			Text: &memoStr,
		}
	}

	txBytes, err := envelope.Tx.MarshalBinary()
	require.NoError(t, err)
	txHash := sha256.Sum256(txBytes)

	result := xdr.TransactionResult{
		Result: xdr.TransactionResultResult{
			Code: xdr.TransactionResultCodeTxSuccess,
		},
	}

	opResults := make([]xdr.OperationResult, 0)
	if !useInvoice {
		opResults = append(opResults, xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeCreateAccount,
				CreateAccountResult: &xdr.CreateAccountResult{
					Code: xdr.CreateAccountResultCodeCreateAccountSuccess,
				},
			},
		})
	}
	for i := 0; i < 5; i++ {
		opResults = append(opResults, xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypePayment,
				PaymentResult: &xdr.PaymentResult{
					Code: xdr.PaymentResultCodePaymentSuccess,
				},
			},
		})
	}
	result.Result.Results = &opResults

	resp.Item.TransactionId = &commonpbv4.TransactionId{Value: txHash[:]}
	rawTx := &transactionpbv4.HistoryItem_StellarTransaction{
		StellarTransaction: &commonpbv4.StellarTransaction{},
	}

	rawTx.StellarTransaction.EnvelopeXdr, err = envelope.MarshalBinary()
	require.NoError(t, err)
	rawTx.StellarTransaction.ResultXdr, err = result.MarshalBinary()
	require.NoError(t, err)

	resp.Item.RawTransaction = rawTx

	payments := make([]Payment, 5)
	readOnlyPayments := make([]ReadOnlyPayment, 5)
	resp.Item.Payments = make([]*transactionpbv4.HistoryItem_Payment, 5)
	for i := 0; i < 5; i++ {
		dest, err := PublicKeyFromString(receivers[i+1].Address())
		require.NoError(t, err)

		payments[i] = Payment{
			Sender:      senderKey,
			Destination: dest,
			Quarks:      10,
		}
		if !sameSource {
			payments[i].Channel = &txSourceKey
		}
		if useInvoice {
			payments[i].Invoice = invoiceList.Invoices[i]
			payments[i].Type = kin.TransactionTypeSpend
		} else {
			payments[i].Memo = memoStr
		}

		readOnlyPayments[i] = ReadOnlyPayment{
			Sender:      payments[i].Sender.Public(),
			Destination: payments[i].Destination,
			Type:        payments[i].Type,
			Quarks:      payments[i].Quarks,
			Invoice:     payments[i].Invoice,
			Memo:        payments[i].Memo,
		}

		resp.Item.Payments[i] = &transactionpbv4.HistoryItem_Payment{
			Source:      &commonpbv4.SolanaAccountId{Value: senderKey.Public()},
			Destination: &commonpbv4.SolanaAccountId{Value: dest},
			Amount:      10,
			Index:       uint32(i),
		}
	}

	return payments, TransactionData{TxID: txHash[:], Payments: readOnlyPayments}, resp
}

func generateV3Payments(t *testing.T, sameSource, useInvoice bool, kinVersion version.KinVersion) ([]Payment, TransactionData, transactionpb.GetTransactionResponse) {
	memoStr := "1-test"
	sender, senderAccount := testutil.GenerateAccountID(t)
	senderKey, err := PrivateKeyFromString(sender.Seed())
	require.NoError(t, err)

	var txSourceKey PrivateKey
	var txSourceAccount xdr.AccountId
	if sameSource {
		txSourceAccount = senderAccount
	} else {
		var txSource *keypair.Full
		txSource, txSourceAccount = testutil.GenerateAccountID(t)
		txSourceKey, err = PrivateKeyFromString(txSource.Seed())
		require.NoError(t, err)
	}

	receivers := testutil.GenerateAccountIDs(t, 6)
	ops := make([]xdr.Operation, 0)
	if !useInvoice {
		ops = append(ops, testutil.GenerateCreateOperation(&senderAccount, receivers[0]))
	}
	for i := 0; i < 5; i++ {
		if kinVersion == 2 {
			issuer, err := testutil.StellarAccountIDFromString(kin.Kin2TestIssuer)
			require.NoError(t, err)

			ops = append(ops, testutil.GenerateKin2PaymentOperation(&senderAccount, receivers[i+1], issuer))
		} else {
			ops = append(ops, testutil.GeneratePaymentOperation(&senderAccount, receivers[i+1]))
		}
	}

	envelope := testutil.GenerateTransactionEnvelope(txSourceAccount, 1, ops)
	resp := transactionpb.GetTransactionResponse{
		State: transactionpb.GetTransactionResponse_SUCCESS,
		Item:  &transactionpb.HistoryItem{},
	}

	var invoiceList *commonpb.InvoiceList
	if useInvoice {
		var hash []byte
		hash, invoiceList = generateInvoiceList(t, 5)
		memo, err := kin.NewMemo(1, kin.TransactionTypeSpend, 1, hash[:])
		require.NoError(t, err)

		envelope.Tx.Memo = xdr.Memo{
			Type: xdr.MemoTypeMemoHash,
			Hash: (*xdr.Hash)(&memo),
		}
		resp.Item.InvoiceList = invoiceList
	} else {
		envelope.Tx.Memo = xdr.Memo{
			Type: xdr.MemoTypeMemoText,
			Text: &memoStr,
		}
	}

	txBytes, err := envelope.Tx.MarshalBinary()
	require.NoError(t, err)
	txHash := sha256.Sum256(txBytes)

	result := xdr.TransactionResult{
		Result: xdr.TransactionResultResult{
			Code: xdr.TransactionResultCodeTxSuccess,
		},
	}

	opResults := make([]xdr.OperationResult, 0)
	if !useInvoice {
		opResults = append(opResults, xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeCreateAccount,
				CreateAccountResult: &xdr.CreateAccountResult{
					Code: xdr.CreateAccountResultCodeCreateAccountSuccess,
				},
			},
		})
	}
	for i := 0; i < 5; i++ {
		opResults = append(opResults, xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypePayment,
				PaymentResult: &xdr.PaymentResult{
					Code: xdr.PaymentResultCodePaymentSuccess,
				},
			},
		})
	}
	result.Result.Results = &opResults

	resp.Item.Hash = &commonpb.TransactionHash{Value: txHash[:]}
	resp.Item.EnvelopeXdr, err = envelope.MarshalBinary()
	require.NoError(t, err)
	resp.Item.ResultXdr, err = result.MarshalBinary()
	require.NoError(t, err)

	payments := make([]Payment, 5)
	readOnlyPayments := make([]ReadOnlyPayment, 5)
	for i := 0; i < 5; i++ {
		dest, err := PublicKeyFromString(receivers[i+1].Address())
		require.NoError(t, err)

		payments[i] = Payment{
			Sender:      senderKey,
			Destination: dest,
			Quarks:      10,
		}
		if !sameSource {
			payments[i].Channel = &txSourceKey
		}
		if useInvoice {
			payments[i].Invoice = invoiceList.Invoices[i]
			payments[i].Type = kin.TransactionTypeSpend
		} else {
			payments[i].Memo = memoStr
		}

		readOnlyPayments[i] = ReadOnlyPayment{
			Sender:      payments[i].Sender.Public(),
			Destination: payments[i].Destination,
			Type:        payments[i].Type,
			Quarks:      payments[i].Quarks,
			Invoice:     payments[i].Invoice,
			Memo:        payments[i].Memo,
		}
	}

	return payments, TransactionData{TxID: txHash[:], Payments: readOnlyPayments}, resp
}

func generateInvoiceList(t *testing.T, n int) (hash []byte, invoiceList *commonpb.InvoiceList) {
	invoiceList = &commonpb.InvoiceList{
		Invoices: make([]*commonpb.Invoice, n),
	}
	for i := 0; i < n; i++ {
		invoiceList.Invoices[i] = &commonpb.Invoice{
			Items: []*commonpb.Invoice_LineItem{
				{
					Title:  fmt.Sprintf("Test%d", i),
					Amount: 10,
					Sku:    []byte("randomsku"),
				},
			},
		}
	}

	bytes, err := proto.Marshal(invoiceList)
	require.NoError(t, err)

	sum224 := sha256.Sum224(bytes)
	return sum224[:], invoiceList
}
