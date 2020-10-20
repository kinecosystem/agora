package client

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/agora-common/headers"
	"github.com/kinecosystem/agora-common/kin"
	agoratestutil "github.com/kinecosystem/agora-common/testutil"
	"github.com/kinecosystem/go/keypair"
	"github.com/pkg/errors"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v3"
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"
	transactionpb "github.com/kinecosystem/agora-api/genproto/transaction/v3"

	"github.com/kinecosystem/agora/pkg/testutil"
	"github.com/kinecosystem/agora/pkg/version"
)

type testEnv struct {
	server   *testServer
	conn     *grpc.ClientConn
	internal *InternalClient
	client   *client
}

func setup(t *testing.T, opts ...ClientOption) (*testEnv, func()) {
	env := &testEnv{
		server: newTestServer(),
	}

	conn, serv, err := agoratestutil.NewServer(
		agoratestutil.WithUnaryServerInterceptor(headers.UnaryServerInterceptor()),
		agoratestutil.WithStreamServerInterceptor(headers.StreamServerInterceptor()),
	)
	require.NoError(t, err)

	serv.RegisterService(func(s *grpc.Server) {
		accountpb.RegisterAccountServer(s, env.server)
		transactionpb.RegisterTransactionServer(s, env.server)
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

	v, err := env.internal.GetBlockchainVersion()
	assert.NoError(t, err)
	assert.Equal(t, version.KinVersion3, v)
}

func TestInternal_BlockchainVersionKin2(t *testing.T) {
	env, cleanup := setup(t, WithKinVersion(version.KinVersion2))
	defer cleanup()

	v, err := env.internal.GetBlockchainVersion()
	assert.NoError(t, err)
	assert.Equal(t, version.KinVersion2, v)
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
	assert.NotNil(t, accountInfo)
	assert.EqualValues(t, 1, accountInfo.SequenceNumber)
	assert.EqualValues(t, 10, accountInfo.Balance)
	assert.NoError(t, err)

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
	assert.NotNil(t, accountInfo)
	assert.EqualValues(t, 1, accountInfo.SequenceNumber)
	assert.EqualValues(t, 10, accountInfo.Balance)
	assert.NoError(t, err)

	priv, err = NewPrivateKey()
	require.NoError(t, err)
	env.server.setError(errors.New("unexpected"), 2)
	assert.NoError(t, env.internal.CreateStellarAccount(context.Background(), priv))

	priv, err = NewPrivateKey()
	require.NoError(t, err)
	env.server.setError(errors.New("unexpected"), 3)
	assert.NotNil(t, env.internal.CreateStellarAccount(context.Background(), priv))
}

func TestInternal_GetTransaction(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	_, err := env.internal.GetTransaction(context.Background(), make([]byte, 32))
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
		_, txData, resp := generatePayments(t, tc.sameSource, tc.useInvoice, version.KinVersion3)

		env.server.mu.Lock()
		env.server.blockchains[version.KinVersion3].gets[string(txData.TxHash)] = resp
		env.server.mu.Unlock()

		actual, err := env.internal.GetTransaction(context.Background(), txData.TxHash)
		assert.NoError(t, err)

		assert.EqualValues(t, txData.TxHash, actual.TxHash)

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

	_, err := env.internal.GetTransaction(context.Background(), make([]byte, 32))
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
		_, txData, resp := generatePayments(t, tc.sameSource, tc.useInvoice, version.KinVersion2)

		env.server.mu.Lock()
		env.server.blockchains[version.KinVersion2].gets[string(txData.TxHash)] = resp
		env.server.mu.Unlock()

		actual, err := env.internal.GetTransaction(context.Background(), txData.TxHash)
		assert.NoError(t, err)

		assert.EqualValues(t, txData.TxHash, actual.TxHash)

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
	assert.EqualValues(t, txHash[:], txData.Hash)
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
	assert.EqualValues(t, txHash[:], txData.Hash)
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
			assert.EqualValues(t, txHash[:], submitResult.Hash)
			assert.Error(t, submitResult.Errors.TxError)
		} else {
			assert.EqualValues(t, txHash[:], submitResult.Hash)
			assert.Error(t, err)
		}
	}
}

func TestInternal_SubmitStellarTransctionKin2(t *testing.T) {
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
	assert.EqualValues(t, txHash[:], txData.Hash)
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
	assert.EqualValues(t, txHash[:], txData.Hash)
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
			assert.EqualValues(t, txHash[:], submitResult.Hash)
			assert.Error(t, submitResult.Errors.TxError)
		} else {
			assert.EqualValues(t, txHash[:], submitResult.Hash)
			assert.Error(t, err)
		}
	}
}

func generatePayments(t *testing.T, sameSource, useInvoice bool, kinVersion version.KinVersion) ([]Payment, TransactionData, transactionpb.GetTransactionResponse) {
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

	return payments, TransactionData{TxHash: txHash[:], Payments: readOnlyPayments}, resp
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
