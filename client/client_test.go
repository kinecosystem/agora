package client

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"math"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"
	transactionpb "github.com/kinecosystem/agora-api/genproto/transaction/v3"
)

func TestClient_AccountManagement(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	r := rand.New(rand.NewSource(0))
	pub, priv, err := ed25519.GenerateKey(r)
	require.NoError(t, err)

	balance, err := env.client.GetBalance(context.Background(), PublicKey(pub))
	assert.Equal(t, ErrAccountDoesNotExist, err)
	assert.Zero(t, balance)

	err = env.client.CreateAccount(context.Background(), PrivateKey(priv))
	assert.NoError(t, err)

	err = env.client.CreateAccount(context.Background(), PrivateKey(priv))
	assert.Equal(t, ErrAccountExists, err)

	balance, err = env.client.GetBalance(context.Background(), PublicKey(pub))
	assert.NoError(t, err)
	assert.EqualValues(t, 10, balance)
}

func TestClient_GetTransaction(t *testing.T) {
	// currently this proxies directly to internal, which has tests.
	// if this changes, we should add more tests here.
}

func TestClient_AppIndexNotSet(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	c, err := New(
		EnvironmentTest,
		WithGRPC(env.conn),
		WithMaxRetries(3),
		WithMinDelay(time.Millisecond),
		WithMaxDelay(time.Millisecond),
	)
	require.NoError(t, err)

	r := rand.New(rand.NewSource(0))

	_, sender, err := ed25519.GenerateKey(r)
	require.NoError(t, err)
	require.NoError(t, env.client.CreateAccount(context.Background(), PrivateKey(sender)))
	_, dest, err := ed25519.GenerateKey(r)
	require.NoError(t, err)
	require.NoError(t, env.client.CreateAccount(context.Background(), PrivateKey(dest)))

	payments := []Payment{
		{
			Sender:      PrivateKey(sender),
			Destination: PrivateKey(dest).Public(),
			Type:        kin.TransactionTypeSpend,
			Quarks:      11,
		},
		{
			Sender:      PrivateKey(sender),
			Destination: PrivateKey(dest).Public(),
			Type:        kin.TransactionTypeSpend,
			Quarks:      11,
			Memo:        "1-test",
		},
	}

	for _, p := range payments {
		_, err = c.SubmitPayment(context.Background(), p)
		assert.NoError(t, err)
	}

	invoicePayment := Payment{
		Sender:      PrivateKey(sender),
		Destination: PrivateKey(dest).Public(),
		Type:        kin.TransactionTypeSpend,
		Quarks:      11,
		Invoice: &commonpb.Invoice{
			Items: []*commonpb.Invoice_LineItem{
				{
					Title:  "test",
					Amount: 11,
				},
			},
		},
	}

	_, err = c.SubmitPayment(context.Background(), invoicePayment)
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "without an app index"))
}

func TestClient_SubmitPayment(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	r := rand.New(rand.NewSource(0))

	_, sender, err := ed25519.GenerateKey(r)
	require.NoError(t, err)
	_, source, err := ed25519.GenerateKey(r)
	require.NoError(t, err)
	_, dest, err := ed25519.GenerateKey(r)
	require.NoError(t, err)

	for _, acc := range [][]byte{sender, source, dest} {
		require.NoError(t, env.client.CreateAccount(context.Background(), PrivateKey(acc)))
	}

	payments := []Payment{
		{
			Sender:      PrivateKey(sender),
			Destination: PrivateKey(dest).Public(),
			Type:        kin.TransactionTypeSpend,
			Quarks:      11,
		},
		{
			Sender:      PrivateKey(sender),
			Destination: PrivateKey(dest).Public(),
			Type:        kin.TransactionTypeSpend,
			Quarks:      11,
			Source:      (*PrivateKey)(&source),
		},
		{
			Sender:      PrivateKey(sender),
			Destination: PrivateKey(dest).Public(),
			Type:        kin.TransactionTypeSpend,
			Quarks:      11,
			Memo:        "1-test",
		},
		{
			Sender:      PrivateKey(sender),
			Destination: PrivateKey(dest).Public(),
			Type:        kin.TransactionTypeSpend,
			Quarks:      11,
			Invoice: &commonpb.Invoice{
				Items: []*commonpb.Invoice_LineItem{
					{
						Title:  "test",
						Amount: 11,
					},
				},
			},
		},
	}

	for _, p := range payments {
		var initSeq int64
		if p.Source == nil {
			info, err := env.internal.GetStellarAccountInfo(context.Background(), p.Sender.Public())
			require.NoError(t, err)
			initSeq = info.SequenceNumber
		} else {
			info, err := env.internal.GetStellarAccountInfo(context.Background(), p.Source.Public())
			require.NoError(t, err)
			initSeq = info.SequenceNumber
		}

		hash, err := env.client.SubmitPayment(context.Background(), p)
		assert.NotNil(t, hash)
		assert.NoError(t, err)

		func() {
			env.server.mu.Lock()
			defer env.server.mu.Unlock()
			defer func() { env.server.submits = nil }()

			assert.Len(t, env.server.submits, 1)

			var envelope xdr.TransactionEnvelope
			assert.NoError(t, envelope.UnmarshalBinary(env.server.submits[0].EnvelopeXdr))

			assert.EqualValues(t, 100, envelope.Tx.Fee)
			assert.EqualValues(t, initSeq+1, envelope.Tx.SeqNum)
			assert.Len(t, envelope.Tx.Operations, 1)
			assert.Nil(t, envelope.Tx.TimeBounds)

			sourceAccount := envelope.Tx.SourceAccount.MustEd25519()
			if p.Source != nil {
				assert.Len(t, envelope.Signatures, 2)
				assert.EqualValues(t, p.Source.Public(), sourceAccount[:])
			} else {
				assert.Len(t, envelope.Signatures, 1)
				assert.EqualValues(t, p.Sender.Public(), sourceAccount[:])
			}

			if p.Memo != "" {
				assert.Equal(t, xdr.MemoTypeMemoText, envelope.Tx.Memo.Type)
				assert.Equal(t, p.Memo, *envelope.Tx.Memo.Text)
			} else if p.Invoice != nil {
				assert.Equal(t, xdr.MemoTypeMemoHash, envelope.Tx.Memo.Type)

				invoiceList := &commonpb.InvoiceList{
					Invoices: []*commonpb.Invoice{p.Invoice},
				}
				ilBytes, err := proto.Marshal(invoiceList)
				require.NoError(t, err)
				ilHash := sha256.Sum224(ilBytes)

				memo, ok := kin.MemoFromXDR(envelope.Tx.Memo, true)
				assert.True(t, ok)

				assert.EqualValues(t, ilHash[:], memo.ForeignKey()[:28])
				assert.True(t, proto.Equal(invoiceList, env.server.submits[0].InvoiceList))
			} else {
				assert.Equal(t, xdr.MemoTypeMemoHash, envelope.Tx.Memo.Type)

				memo, ok := kin.MemoFromXDR(envelope.Tx.Memo, true)
				assert.True(t, ok)

				assert.EqualValues(t, 1, memo.AppIndex())
				assert.EqualValues(t, make([]byte, 29), memo.ForeignKey())
			}
		}()
	}

	env.server.mu.Lock()
	env.server.submitResponses = []*transactionpb.SubmitTransactionResponse{
		{
			Result: transactionpb.SubmitTransactionResponse_INVOICE_ERROR,
			InvoiceErrors: []*transactionpb.SubmitTransactionResponse_InvoiceError{
				{
					Invoice: payments[3].Invoice,
					Reason:  transactionpb.SubmitTransactionResponse_InvoiceError_ALREADY_PAID,
				},
			},
		},
		{
			Result: transactionpb.SubmitTransactionResponse_INVOICE_ERROR,
			InvoiceErrors: []*transactionpb.SubmitTransactionResponse_InvoiceError{
				{
					Invoice: payments[3].Invoice,
					Reason:  transactionpb.SubmitTransactionResponse_InvoiceError_WRONG_DESTINATION,
				},
			},
		},
		{
			Result: transactionpb.SubmitTransactionResponse_INVOICE_ERROR,
			InvoiceErrors: []*transactionpb.SubmitTransactionResponse_InvoiceError{
				{
					Invoice: payments[3].Invoice,
					Reason:  transactionpb.SubmitTransactionResponse_InvoiceError_SKU_NOT_FOUND,
				},
			},
		},
	}
	env.server.mu.Unlock()

	for _, e := range []error{ErrAlreadyPaid, ErrWrongDestination, ErrSKUNotFound} {
		hash, err := env.client.SubmitPayment(context.Background(), payments[3])
		assert.NotNil(t, hash)
		assert.Equal(t, e, err)
	}

	txFailedResult := xdr.TransactionResult{
		Result: xdr.TransactionResultResult{
			Code: xdr.TransactionResultCodeTxBadAuth,
		},
	}
	resultBytes, err := txFailedResult.MarshalBinary()
	require.NoError(t, err)
	env.server.mu.Lock()
	env.server.submitResponses = []*transactionpb.SubmitTransactionResponse{
		{
			Result:    transactionpb.SubmitTransactionResponse_FAILED,
			ResultXdr: resultBytes,
		},
	}
	env.server.mu.Unlock()

	hash, err := env.client.SubmitPayment(context.Background(), payments[3])
	assert.NotNil(t, hash)
	assert.Equal(t, ErrInvalidSignature, err)

	opResults := []xdr.OperationResult{
		{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypePayment,
				PaymentResult: &xdr.PaymentResult{
					Code: xdr.PaymentResultCodePaymentUnderfunded,
				},
			},
		},
	}
	opFailedResult := xdr.TransactionResult{
		Result: xdr.TransactionResultResult{
			Code:    xdr.TransactionResultCodeTxFailed,
			Results: &opResults,
		},
	}
	resultBytes, err = opFailedResult.MarshalBinary()
	require.NoError(t, err)

	env.server.mu.Lock()
	env.server.submitResponses = []*transactionpb.SubmitTransactionResponse{
		{
			Result:    transactionpb.SubmitTransactionResponse_FAILED,
			Hash:      nil,
			ResultXdr: resultBytes,
		},
	}
	env.server.mu.Unlock()

	hash, err = env.client.SubmitPayment(context.Background(), payments[3])
	assert.NotNil(t, hash)
	assert.Equal(t, ErrInsufficientBalance, err)
}

func TestClient_SendEarnBatchInternal(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	r := rand.New(rand.NewSource(0))

	_, sender, err := ed25519.GenerateKey(r)
	require.NoError(t, err)
	_, source, err := ed25519.GenerateKey(r)
	require.NoError(t, err)

	earnAccounts := make([]PrivateKey, 5)
	for i := 0; i < len(earnAccounts); i++ {
		_, dest, err := ed25519.GenerateKey(r)
		require.NoError(t, err)
		earnAccounts[i] = PrivateKey(dest)
	}

	for _, acc := range append([]PrivateKey{PrivateKey(sender), PrivateKey(source)}, earnAccounts...) {
		require.NoError(t, env.client.CreateAccount(context.Background(), acc))
	}

	var earns []Earn
	for i, r := range earnAccounts {
		earns = append(earns, Earn{
			Destination: PrivateKey(r).Public(),
			Quarks:      int64(i) + 1,
		})
	}

	var invoiceEarns []Earn
	for i, r := range earnAccounts {
		invoiceEarns = append(invoiceEarns, Earn{
			Destination: PrivateKey(r).Public(),
			Quarks:      int64(i) + 1,
			Invoice: &commonpb.Invoice{
				Items: []*commonpb.Invoice_LineItem{
					{
						Title:  "test",
						Amount: 100,
					},
				},
			},
		})
	}

	var mixedEarns []Earn
	for i, r := range earnAccounts {
		e := Earn{
			Destination: PrivateKey(r).Public(),
			Quarks:      int64(i) + 1,
		}
		if i%2 == 0 {
			e.Invoice = &commonpb.Invoice{
				Items: []*commonpb.Invoice_LineItem{
					{
						Title:  "test",
						Amount: 100,
					},
				},
			}
		}
		mixedEarns = append(mixedEarns, e)
	}

	batches := []EarnBatch{
		{
			Sender: PrivateKey(sender),
			Earns:  earns,
		},
		{
			Sender: PrivateKey(sender),
			Source: (*PrivateKey)(&source),
			Earns:  earns,
		},
		{
			Sender: PrivateKey(sender),
			Memo:   "1-test",
			Earns:  earns,
		},
		{
			Sender: PrivateKey(sender),
			Earns:  invoiceEarns,
		},
	}

	for _, b := range batches {
		var initSeq int64
		if b.Source == nil {
			info, err := env.internal.GetStellarAccountInfo(context.Background(), b.Sender.Public())
			require.NoError(t, err)
			initSeq = info.SequenceNumber
		} else {
			info, err := env.internal.GetStellarAccountInfo(context.Background(), b.Source.Public())
			require.NoError(t, err)
			initSeq = info.SequenceNumber
		}

		result, err := env.client.sendEarnBatch(context.Background(), b)
		assert.NoError(t, err)

		func() {
			env.server.mu.Lock()
			defer env.server.mu.Unlock()
			defer func() { env.server.submits = nil }()

			assert.Len(t, env.server.submits, 1)

			var envelope xdr.TransactionEnvelope
			assert.NoError(t, envelope.UnmarshalBinary(env.server.submits[0].EnvelopeXdr))

			txBytes, err := envelope.Tx.MarshalBinary()
			assert.NoError(t, err)
			txHash := sha256.Sum256(txBytes)
			assert.EqualValues(t, txHash[:], result.Hash)

			assert.EqualValues(t, 100*len(b.Earns), envelope.Tx.Fee)
			assert.EqualValues(t, initSeq+1, envelope.Tx.SeqNum)
			assert.Len(t, envelope.Tx.Operations, len(b.Earns))
			assert.Nil(t, envelope.Tx.TimeBounds)

			sourceAccount := envelope.Tx.SourceAccount.MustEd25519()
			if b.Source != nil {
				assert.Len(t, envelope.Signatures, 2)
				assert.EqualValues(t, b.Source.Public(), sourceAccount[:])
			} else {
				assert.Len(t, envelope.Signatures, 1)
				assert.EqualValues(t, b.Sender.Public(), sourceAccount[:])
			}

			if b.Memo != "" {
				assert.Equal(t, xdr.MemoTypeMemoText, envelope.Tx.Memo.Type)
				assert.Equal(t, b.Memo, *envelope.Tx.Memo.Text)
			} else if b.Earns[0].Invoice == nil {
				assert.Equal(t, xdr.MemoTypeMemoHash, envelope.Tx.Memo.Type)

				memo, ok := kin.MemoFromXDR(envelope.Tx.Memo, true)
				assert.True(t, ok)

				assert.EqualValues(t, 1, memo.AppIndex())
				assert.EqualValues(t, make([]byte, 29), memo.ForeignKey())
			} else {
				assert.Equal(t, xdr.MemoTypeMemoHash, envelope.Tx.Memo.Type)

				invoiceList := &commonpb.InvoiceList{
					Invoices: []*commonpb.Invoice{},
				}
				for _, r := range b.Earns {
					invoiceList.Invoices = append(invoiceList.Invoices, r.Invoice)
				}

				ilBytes, err := proto.Marshal(invoiceList)
				require.NoError(t, err)
				ilHash := sha256.Sum224(ilBytes)

				memo, ok := kin.MemoFromXDR(envelope.Tx.Memo, true)
				assert.True(t, ok)

				assert.EqualValues(t, ilHash[:], memo.ForeignKey()[:28])
				assert.True(t, proto.Equal(invoiceList, env.server.submits[0].InvoiceList))
			}
		}()
	}

	badBatches := []EarnBatch{
		{
			Sender: PrivateKey(sender),
			Earns:  mixedEarns,
		},
	}
	for _, b := range badBatches {
		_, err := env.client.sendEarnBatch(context.Background(), b)
		assert.NotNil(t, err)
	}
}

func TestClient_SendEarnBatch(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	r := rand.New(rand.NewSource(0))

	_, sender, err := ed25519.GenerateKey(r)
	require.NoError(t, err)
	_, source, err := ed25519.GenerateKey(r)
	require.NoError(t, err)

	earnAccounts := make([]PrivateKey, 202)
	for i := 0; i < len(earnAccounts); i++ {
		_, dest, err := ed25519.GenerateKey(r)
		require.NoError(t, err)
		earnAccounts[i] = PrivateKey(dest)
	}

	for _, acc := range append([]PrivateKey{PrivateKey(sender), PrivateKey(source)}, earnAccounts...) {
		require.NoError(t, env.client.CreateAccount(context.Background(), acc))
	}

	var earns []Earn
	for i, r := range earnAccounts {
		earns = append(earns, Earn{
			Destination: PrivateKey(r).Public(),
			Quarks:      int64(i) + 1,
		})
	}
	var invoiceEarns []Earn
	for i, r := range earnAccounts {
		invoiceEarns = append(invoiceEarns, Earn{
			Destination: PrivateKey(r).Public(),
			Quarks:      int64(i) + 1,
			Invoice: &commonpb.Invoice{
				Items: []*commonpb.Invoice_LineItem{
					{
						Title:  "Test",
						Amount: int64(i) + 1,
					},
				},
			},
		})
	}

	batches := []EarnBatch{
		{
			Sender: PrivateKey(sender),
			Earns:  earns,
		},
		{
			Sender: PrivateKey(sender),
			Source: (*PrivateKey)(&source),
			Earns:  earns,
		},
		{
			Sender: PrivateKey(sender),
			Earns:  invoiceEarns,
		},
	}

	for _, b := range batches {
		var initSeq int64
		if b.Source == nil {
			info, err := env.internal.GetStellarAccountInfo(context.Background(), b.Sender.Public())
			require.NoError(t, err)
			initSeq = info.SequenceNumber
		} else {
			info, err := env.internal.GetStellarAccountInfo(context.Background(), b.Source.Public())
			require.NoError(t, err)
			initSeq = info.SequenceNumber
		}

		result, err := env.client.SendEarnBatch(context.Background(), b)
		assert.NoError(t, err)

		func() {
			env.server.mu.Lock()
			defer env.server.mu.Unlock()
			defer func() { env.server.submits = nil }()

			assert.Len(t, env.server.submits, 3)
			assert.Len(t, result.Succeeded, len(earnAccounts))
			assert.Empty(t, result.Failed)

			txHashes := make(map[string]struct{})
			for i, r := range result.Succeeded {
				txHashes[string(r.TxHash)] = struct{}{}
				assert.NoError(t, r.Error)
				assert.Equal(t, b.Earns[i], r.Earn)
			}
			assert.Len(t, txHashes, 3)

			for i, s := range env.server.submits {
				var envelope xdr.TransactionEnvelope
				assert.NoError(t, envelope.UnmarshalBinary(s.EnvelopeXdr))

				txBytes, err := envelope.Tx.MarshalBinary()
				assert.NoError(t, err)
				txHash := sha256.Sum256(txBytes)
				_, exists := txHashes[string(txHash[:])]
				assert.True(t, exists)

				batchSize := int(math.Min(100, float64(len(b.Earns)-i*100)))

				assert.EqualValues(t, 100*batchSize, envelope.Tx.Fee)
				assert.EqualValues(t, initSeq+int64(i)+1, envelope.Tx.SeqNum)
				assert.Len(t, envelope.Tx.Operations, batchSize)
				assert.Nil(t, envelope.Tx.TimeBounds)

				sourceAccount := envelope.Tx.SourceAccount.MustEd25519()
				if b.Source != nil {
					assert.Len(t, envelope.Signatures, 2)
					assert.EqualValues(t, b.Source.Public(), sourceAccount[:])
				} else {
					assert.Len(t, envelope.Signatures, 1)
					assert.EqualValues(t, b.Sender.Public(), sourceAccount[:])
				}

				if b.Memo != "" {
					assert.Equal(t, xdr.MemoTypeMemoText, envelope.Tx.Memo.Type)
					assert.Equal(t, b.Memo, *envelope.Tx.Memo.Text)
				} else if b.Earns[0].Invoice == nil {
					assert.Equal(t, xdr.MemoTypeMemoHash, envelope.Tx.Memo.Type)

					memo, ok := kin.MemoFromXDR(envelope.Tx.Memo, true)
					assert.True(t, ok)

					assert.EqualValues(t, 1, memo.AppIndex())
					assert.EqualValues(t, make([]byte, 29), memo.ForeignKey())
				} else {
					assert.Equal(t, xdr.MemoTypeMemoHash, envelope.Tx.Memo.Type)

					invoiceList := &commonpb.InvoiceList{}

					start := i * 100
					end := int(math.Min(float64((i+1)*100), float64(len(b.Earns))))
					for j := start; j < end; j++ {
						invoiceList.Invoices = append(invoiceList.Invoices, b.Earns[j].Invoice)
					}

					ilBytes, err := proto.Marshal(invoiceList)
					require.NoError(t, err)
					ilHash := sha256.Sum224(ilBytes)

					memo, ok := kin.MemoFromXDR(envelope.Tx.Memo, true)
					assert.True(t, ok)

					assert.EqualValues(t, ilHash[:], memo.ForeignKey()[:28])
					assert.True(t, proto.Equal(invoiceList, s.InvoiceList))
				}
			}
		}()
	}

	// Ensure context cancellation works correctly.
	ctx, cancelFunc := context.WithCancel(context.Background())
	cancelFunc()
	result, err := env.client.SendEarnBatch(ctx, batches[0])
	assert.Error(t, err)
	assert.Empty(t, result.Succeeded, len(batches[0].Earns))
	assert.Len(t, result.Failed, len(batches[0].Earns))

	// Ensure that error handling for the following cases is correct:
	//   - Partial Success: (some of the batches succeeded)
	//   - Op Failures: operation failures are reported
	//   - Tx Failures: transaction level failures
	txFailedResult := xdr.TransactionResult{
		Result: xdr.TransactionResultResult{
			Code: xdr.TransactionResultCodeTxBadAuth,
		},
	}
	resultBytes, err := txFailedResult.MarshalBinary()
	require.NoError(t, err)
	env.server.mu.Lock()
	env.server.submitResponses = []*transactionpb.SubmitTransactionResponse{
		nil,
		{
			Result:    transactionpb.SubmitTransactionResponse_FAILED,
			Hash:      nil, // w/e
			ResultXdr: resultBytes,
		},
	}
	env.server.mu.Unlock()

	result, err = env.client.SendEarnBatch(context.Background(), batches[0])
	assert.Error(t, err)
	assert.Len(t, result.Succeeded, 100)
	assert.Len(t, result.Failed, 102)
	for i := 0; i < len(result.Succeeded); i++ {
		assert.NoError(t, result.Succeeded[i].Error)
		assert.NotNil(t, result.Succeeded[i].TxHash)
		assert.Equal(t, batches[0].Earns[i], result.Succeeded[i].Earn)
	}
	for i := 0; i < len(result.Failed); i++ {
		assert.NoError(t, result.Failed[i].Error)
		assert.Equal(t, batches[0].Earns[100+i], result.Failed[i].Earn)
	}

	opResults := make([]xdr.OperationResult, 100)
	for i := 0; i < 100; i++ {
		opResults[i] = xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypePayment,
				PaymentResult: &xdr.PaymentResult{
					Code: xdr.PaymentResultCodePaymentUnderfunded,
				},
			},
		}
	}
	opFailedResult := xdr.TransactionResult{
		Result: xdr.TransactionResultResult{
			Code:    xdr.TransactionResultCodeTxFailed,
			Results: &opResults,
		},
	}
	resultBytes, err = opFailedResult.MarshalBinary()
	require.NoError(t, err)

	env.server.mu.Lock()
	env.server.submitResponses = []*transactionpb.SubmitTransactionResponse{
		nil,
		{
			Result:    transactionpb.SubmitTransactionResponse_FAILED,
			Hash:      nil,
			ResultXdr: resultBytes,
		},
	}
	env.server.mu.Unlock()

	result, err = env.client.SendEarnBatch(context.Background(), batches[0])
	assert.Error(t, err)
	assert.Len(t, result.Succeeded, 100)
	assert.Len(t, result.Failed, 102)
	for i := 0; i < len(result.Succeeded); i++ {
		assert.NoError(t, result.Succeeded[i].Error)
		assert.NotNil(t, result.Succeeded[i].TxHash)
		assert.Equal(t, batches[0].Earns[i], result.Succeeded[i].Earn)
	}
	for i := 0; i < 100; i++ {
		assert.NotNil(t, result.Failed[i].Error)
		assert.Equal(t, batches[0].Earns[100+i], result.Failed[i].Earn)
	}
	for i := 100; i < 102; i++ {
		assert.Nil(t, result.Failed[i].Error)
		assert.Equal(t, batches[0].Earns[100+i], result.Failed[i].Earn)
	}
}
