package client

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/memo"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/kinecosystem/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	accountpbv4 "github.com/kinecosystem/agora-api/genproto/account/v4"
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"
	commonpbv4 "github.com/kinecosystem/agora-api/genproto/common/v4"
	transactionpb "github.com/kinecosystem/agora-api/genproto/transaction/v3"
	transactionpbv4 "github.com/kinecosystem/agora-api/genproto/transaction/v4"

	"github.com/kinecosystem/agora/pkg/testutil"
	"github.com/kinecosystem/agora/pkg/version"
)

func TestClient_AccountManagement(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	priv, err := NewPrivateKey()
	require.NoError(t, err)

	balance, err := env.client.GetBalance(context.Background(), priv.Public())
	assert.Equal(t, ErrAccountDoesNotExist, err)
	assert.Zero(t, balance)

	err = env.client.CreateAccount(context.Background(), PrivateKey(priv))
	assert.NoError(t, err)

	err = env.client.CreateAccount(context.Background(), PrivateKey(priv))
	assert.Equal(t, ErrAccountExists, err)

	balance, err = env.client.GetBalance(context.Background(), priv.Public())
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

	sender, err := NewPrivateKey()
	require.NoError(t, err)
	require.NoError(t, env.client.CreateAccount(context.Background(), sender))
	dest, err := NewPrivateKey()
	require.NoError(t, err)
	require.NoError(t, env.client.CreateAccount(context.Background(), dest))

	payments := []Payment{
		{
			Sender:      sender,
			Destination: dest.Public(),
			Type:        kin.TransactionTypeSpend,
			Quarks:      11,
		},
		{
			Sender:      sender,
			Destination: dest.Public(),
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
		Sender:      sender,
		Destination: dest.Public(),
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

	sender, err := NewPrivateKey()
	require.NoError(t, err)
	channel, err := NewPrivateKey()
	require.NoError(t, err)
	dest, err := NewPrivateKey()
	require.NoError(t, err)

	for _, acc := range [][]byte{sender, channel, dest} {
		require.NoError(t, env.client.CreateAccount(context.Background(), acc))
	}

	payments := []Payment{
		{
			Sender:      sender,
			Destination: dest.Public(),
			Type:        kin.TransactionTypeSpend,
			Quarks:      11,
		},
		{
			Sender:      sender,
			Destination: dest.Public(),
			Type:        kin.TransactionTypeSpend,
			Quarks:      11,
			Channel:     &channel,
		},
		{
			Sender:      sender,
			Destination: dest.Public(),
			Type:        kin.TransactionTypeSpend,
			Quarks:      11,
			Memo:        "1-test",
		},
		{
			Sender:      sender,
			Destination: dest.Public(),
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
		if p.Channel == nil {
			info, err := env.internal.GetStellarAccountInfo(context.Background(), p.Sender.Public())
			require.NoError(t, err)
			initSeq = info.SequenceNumber
		} else {
			info, err := env.internal.GetStellarAccountInfo(context.Background(), p.Channel.Public())
			require.NoError(t, err)
			initSeq = info.SequenceNumber
		}

		hash, err := env.client.SubmitPayment(context.Background(), p)
		assert.NotNil(t, hash)
		assert.NoError(t, err)

		func() {
			env.server.mu.Lock()
			defer env.server.mu.Unlock()
			defer func() { env.server.blockchains[version.KinVersion3].submits = nil }()

			assert.Len(t, env.server.blockchains[version.KinVersion3].submits, 1)

			var envelope xdr.TransactionEnvelope
			assert.NoError(t, envelope.UnmarshalBinary(env.server.blockchains[version.KinVersion3].submits[0].EnvelopeXdr))

			assert.EqualValues(t, 100, envelope.Tx.Fee)
			assert.EqualValues(t, initSeq+1, envelope.Tx.SeqNum)
			assert.Len(t, envelope.Tx.Operations, 1)

			assert.EqualValues(t, xdr.Int64(11), envelope.Tx.Operations[0].Body.PaymentOp.Amount)
			assert.EqualValues(t, xdr.AssetTypeAssetTypeNative, envelope.Tx.Operations[0].Body.PaymentOp.Asset.Type)

			assert.Nil(t, envelope.Tx.TimeBounds)

			sourceAccount := envelope.Tx.SourceAccount.MustEd25519()
			if p.Channel != nil {
				assert.Len(t, envelope.Signatures, 2)
				assert.EqualValues(t, p.Channel.Public(), sourceAccount[:])
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
				assert.True(t, proto.Equal(invoiceList, env.server.blockchains[version.KinVersion3].submits[0].InvoiceList))
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
	env.server.blockchains[version.KinVersion3].submitResponses = []*transactionpb.SubmitTransactionResponse{
		{
			Result: transactionpb.SubmitTransactionResponse_INVOICE_ERROR,
			InvoiceErrors: []*commonpb.InvoiceError{
				{
					Invoice: payments[3].Invoice,
					Reason:  commonpb.InvoiceError_ALREADY_PAID,
				},
			},
		},
		{
			Result: transactionpb.SubmitTransactionResponse_INVOICE_ERROR,
			InvoiceErrors: []*commonpb.InvoiceError{
				{
					Invoice: payments[3].Invoice,
					Reason:  commonpb.InvoiceError_WRONG_DESTINATION,
				},
			},
		},
		{
			Result: transactionpb.SubmitTransactionResponse_INVOICE_ERROR,
			InvoiceErrors: []*commonpb.InvoiceError{
				{
					Invoice: payments[3].Invoice,
					Reason:  commonpb.InvoiceError_SKU_NOT_FOUND,
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
	env.server.blockchains[version.KinVersion3].submitResponses = []*transactionpb.SubmitTransactionResponse{
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
	env.server.blockchains[version.KinVersion3].submitResponses = []*transactionpb.SubmitTransactionResponse{
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

func TestClient_SubmitPaymentKin2(t *testing.T) {
	env, cleanup := setup(t, WithKinVersion(version.KinVersion2))
	defer cleanup()

	sender, err := NewPrivateKey()
	require.NoError(t, err)
	dest, err := NewPrivateKey()
	require.NoError(t, err)

	for _, acc := range [][]byte{sender, dest} {
		require.NoError(t, env.client.CreateAccount(context.Background(), acc))
	}
	payment := Payment{
		Sender:      sender,
		Destination: dest.Public(),
		Type:        kin.TransactionTypeSpend,
		Quarks:      11,
	}

	info, err := env.internal.GetStellarAccountInfo(context.Background(), sender.Public())
	require.NoError(t, err)
	initSeq := info.SequenceNumber
	hash, err := env.client.SubmitPayment(context.Background(), payment)
	assert.NotNil(t, hash)
	assert.NoError(t, err)

	func() {
		env.server.mu.Lock()
		defer env.server.mu.Unlock()
		defer func() { env.server.blockchains[version.KinVersion2].submits = nil }()

		assert.Len(t, env.server.blockchains[version.KinVersion2].submits, 1)

		var envelope xdr.TransactionEnvelope
		assert.NoError(t, envelope.UnmarshalBinary(env.server.blockchains[version.KinVersion2].submits[0].EnvelopeXdr))

		assert.EqualValues(t, 100, envelope.Tx.Fee)
		assert.EqualValues(t, initSeq+1, envelope.Tx.SeqNum)
		assert.Len(t, envelope.Tx.Operations, 1)
		assert.Nil(t, envelope.Tx.TimeBounds)

		// Assert amount and proper asset
		assert.EqualValues(t, xdr.Int64(11*100), envelope.Tx.Operations[0].Body.PaymentOp.Amount)
		assert.EqualValues(t, xdr.AssetTypeAssetTypeCreditAlphanum4, envelope.Tx.Operations[0].Body.PaymentOp.Asset.Type)
		assert.EqualValues(t, kinAssetCode, envelope.Tx.Operations[0].Body.PaymentOp.Asset.AlphaNum4.AssetCode)

		expectedIssuer, err := testutil.StellarAccountIDFromString(kin.Kin2TestIssuer)
		require.NoError(t, err)
		assert.EqualValues(t, expectedIssuer, envelope.Tx.Operations[0].Body.PaymentOp.Asset.AlphaNum4.Issuer)

		sourceAccount := envelope.Tx.SourceAccount.MustEd25519()
		assert.Len(t, envelope.Signatures, 1)
		assert.EqualValues(t, payment.Sender.Public(), sourceAccount[:])

		assert.Equal(t, xdr.MemoTypeMemoHash, envelope.Tx.Memo.Type)

		memo, ok := kin.MemoFromXDR(envelope.Tx.Memo, true)
		assert.True(t, ok)

		assert.EqualValues(t, 1, memo.AppIndex())
		assert.EqualValues(t, make([]byte, 29), memo.ForeignKey())
	}()
}

func TestClient_SubmitPaymentDuplicateSigners(t *testing.T) {
	sender, err := NewPrivateKey()
	require.NoError(t, err)
	dest, err := NewPrivateKey()
	require.NoError(t, err)

	env, cleanup := setup(t, WithWhitelister(sender))
	defer cleanup()

	for _, acc := range [][]byte{sender, dest} {
		require.NoError(t, env.client.CreateAccount(context.Background(), acc))
	}

	p := Payment{
		Sender:      sender,
		Destination: dest.Public(),
		Type:        kin.TransactionTypeSpend,
		Quarks:      11,
		Channel:     &sender,
	}

	info, err := env.internal.GetStellarAccountInfo(context.Background(), p.Sender.Public())
	require.NoError(t, err)
	initSeq := info.SequenceNumber

	hash, err := env.client.SubmitPayment(context.Background(), p)
	assert.NotNil(t, hash)
	assert.NoError(t, err)

	env.server.mu.Lock()
	defer env.server.mu.Unlock()

	assert.Len(t, env.server.blockchains[version.KinVersion3].submits, 1)

	var envelope xdr.TransactionEnvelope
	assert.NoError(t, envelope.UnmarshalBinary(env.server.blockchains[version.KinVersion3].submits[0].EnvelopeXdr))

	assert.EqualValues(t, 100, envelope.Tx.Fee)
	assert.EqualValues(t, initSeq+1, envelope.Tx.SeqNum)
	assert.Len(t, envelope.Tx.Operations, 1)

	assert.EqualValues(t, xdr.Int64(11), envelope.Tx.Operations[0].Body.PaymentOp.Amount)
	assert.EqualValues(t, xdr.AssetTypeAssetTypeNative, envelope.Tx.Operations[0].Body.PaymentOp.Asset.Type)

	assert.Nil(t, envelope.Tx.TimeBounds)

	sourceAccount := envelope.Tx.SourceAccount.MustEd25519()
	assert.EqualValues(t, p.Sender.Public(), sourceAccount[:])

	// There should only be one signature despite channel + whitelister being set
	assert.Len(t, envelope.Signatures, 1)

	assert.Equal(t, xdr.MemoTypeMemoHash, envelope.Tx.Memo.Type)

	memo, ok := kin.MemoFromXDR(envelope.Tx.Memo, true)
	assert.True(t, ok)

	assert.EqualValues(t, 1, memo.AppIndex())
	assert.EqualValues(t, make([]byte, 29), memo.ForeignKey())
}

func TestClient_SubmitEarnBatchInternal(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	sender, err := NewPrivateKey()
	require.NoError(t, err)
	channel, err := NewPrivateKey()
	require.NoError(t, err)

	earnAccounts := make([]PrivateKey, 5)
	for i := 0; i < len(earnAccounts); i++ {
		dest, err := NewPrivateKey()
		require.NoError(t, err)
		earnAccounts[i] = dest
	}

	for _, acc := range append([]PrivateKey{sender, channel}, earnAccounts...) {
		require.NoError(t, env.client.CreateAccount(context.Background(), acc))
	}

	var earns []Earn
	for i, r := range earnAccounts {
		earns = append(earns, Earn{
			Destination: r.Public(),
			Quarks:      int64(i) + 1,
		})
	}

	var invoiceEarns []Earn
	for i, r := range earnAccounts {
		invoiceEarns = append(invoiceEarns, Earn{
			Destination: r.Public(),
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
			Destination: r.Public(),
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
			Sender: sender,
			Earns:  earns,
		},
		{
			Sender:  sender,
			Channel: &channel,
			Earns:   earns,
		},
		{
			Sender: sender,
			Memo:   "1-test",
			Earns:  earns,
		},
		{
			Sender: sender,
			Earns:  invoiceEarns,
		},
	}

	for _, b := range batches {
		var initSeq int64
		if b.Channel == nil {
			info, err := env.internal.GetStellarAccountInfo(context.Background(), b.Sender.Public())
			require.NoError(t, err)
			initSeq = info.SequenceNumber
		} else {
			info, err := env.internal.GetStellarAccountInfo(context.Background(), b.Channel.Public())
			require.NoError(t, err)
			initSeq = info.SequenceNumber
		}

		result, err := env.client.submitEarnBatch(context.Background(), b)
		assert.NoError(t, err)

		func() {
			env.server.mu.Lock()
			defer env.server.mu.Unlock()
			defer func() { env.server.blockchains[version.KinVersion3].submits = nil }()

			assert.Len(t, env.server.blockchains[version.KinVersion3].submits, 1)

			var envelope xdr.TransactionEnvelope
			assert.NoError(t, envelope.UnmarshalBinary(env.server.blockchains[version.KinVersion3].submits[0].EnvelopeXdr))

			txBytes, err := envelope.Tx.MarshalBinary()
			assert.NoError(t, err)
			txHash := sha256.Sum256(txBytes)
			assert.EqualValues(t, txHash[:], result.ID)

			assert.EqualValues(t, 100*len(b.Earns), envelope.Tx.Fee)
			assert.EqualValues(t, initSeq+1, envelope.Tx.SeqNum)
			assert.Len(t, envelope.Tx.Operations, len(b.Earns))
			assert.Nil(t, envelope.Tx.TimeBounds)

			sourceAccount := envelope.Tx.SourceAccount.MustEd25519()
			if b.Channel != nil {
				assert.Len(t, envelope.Signatures, 2)
				assert.EqualValues(t, b.Channel.Public(), sourceAccount[:])
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
				assert.True(t, proto.Equal(invoiceList, env.server.blockchains[version.KinVersion3].submits[0].InvoiceList))
			}
		}()
	}

	badBatches := []EarnBatch{
		{
			Sender: sender,
			Earns:  mixedEarns,
		},
	}
	for _, b := range badBatches {
		_, err := env.client.submitEarnBatch(context.Background(), b)
		assert.NotNil(t, err)
	}
}

func TestClient_SubmitEarnBatchInternalKin2(t *testing.T) {
	env, cleanup := setup(t, WithKinVersion(version.KinVersion2))
	defer cleanup()

	sender, err := NewPrivateKey()
	require.NoError(t, err)
	channel, err := NewPrivateKey()
	require.NoError(t, err)

	earnAccounts := make([]PrivateKey, 5)
	for i := 0; i < len(earnAccounts); i++ {
		dest, err := NewPrivateKey()
		require.NoError(t, err)
		earnAccounts[i] = dest
	}

	for _, acc := range append([]PrivateKey{sender, channel}, earnAccounts...) {
		require.NoError(t, env.client.CreateAccount(context.Background(), acc))
	}

	var earns []Earn
	for i, r := range earnAccounts {
		earns = append(earns, Earn{
			Destination: r.Public(),
			Quarks:      int64(i) + 1,
		})
	}

	b := EarnBatch{
		Sender: sender,
		Earns:  earns,
	}

	info, err := env.internal.GetStellarAccountInfo(context.Background(), b.Sender.Public())
	require.NoError(t, err)
	initSeq := info.SequenceNumber

	result, err := env.client.submitEarnBatch(context.Background(), b)
	assert.NoError(t, err)

	func() {
		env.server.mu.Lock()
		defer env.server.mu.Unlock()
		defer func() { env.server.blockchains[version.KinVersion2].submits = nil }()

		assert.Len(t, env.server.blockchains[version.KinVersion2].submits, 1)

		var envelope xdr.TransactionEnvelope
		assert.NoError(t, envelope.UnmarshalBinary(env.server.blockchains[version.KinVersion2].submits[0].EnvelopeXdr))

		txBytes, err := envelope.Tx.MarshalBinary()
		assert.NoError(t, err)
		txHash := sha256.Sum256(txBytes)
		assert.EqualValues(t, txHash[:], result.ID)

		assert.EqualValues(t, 100*len(b.Earns), envelope.Tx.Fee)
		assert.EqualValues(t, initSeq+1, envelope.Tx.SeqNum)
		assert.Len(t, envelope.Tx.Operations, len(b.Earns))
		assert.Nil(t, envelope.Tx.TimeBounds)

		expectedIssuer, err := testutil.StellarAccountIDFromString(kin.Kin2TestIssuer)
		require.NoError(t, err)

		// Assert amount and proper asset
		for i, op := range envelope.Tx.Operations {
			assert.EqualValues(t, xdr.Int64((i+1)*100), op.Body.PaymentOp.Amount)
			assert.EqualValues(t, xdr.AssetTypeAssetTypeCreditAlphanum4, op.Body.PaymentOp.Asset.Type)
			assert.EqualValues(t, kinAssetCode, op.Body.PaymentOp.Asset.AlphaNum4.AssetCode)
			assert.EqualValues(t, expectedIssuer, op.Body.PaymentOp.Asset.AlphaNum4.Issuer)
		}

		sourceAccount := envelope.Tx.SourceAccount.MustEd25519()

		assert.Len(t, envelope.Signatures, 1)
		assert.EqualValues(t, b.Sender.Public(), sourceAccount[:])

		assert.Equal(t, xdr.MemoTypeMemoHash, envelope.Tx.Memo.Type)

		memo, ok := kin.MemoFromXDR(envelope.Tx.Memo, true)
		assert.True(t, ok)

		assert.EqualValues(t, 1, memo.AppIndex())
		assert.EqualValues(t, make([]byte, 29), memo.ForeignKey())
	}()
}

func TestClient_SubmitEarnBatch(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	sender, err := NewPrivateKey()
	require.NoError(t, err)
	channel, err := NewPrivateKey()
	require.NoError(t, err)

	earnAccounts := make([]PrivateKey, 202)
	for i := 0; i < len(earnAccounts); i++ {
		dest, err := NewPrivateKey()
		require.NoError(t, err)
		earnAccounts[i] = dest
	}

	for _, acc := range append([]PrivateKey{sender, channel}, earnAccounts...) {
		require.NoError(t, env.client.CreateAccount(context.Background(), acc))
	}

	var earns []Earn
	for i, r := range earnAccounts {
		earns = append(earns, Earn{
			Destination: r.Public(),
			Quarks:      int64(i) + 1,
		})
	}
	var invoiceEarns []Earn
	for i, r := range earnAccounts {
		invoiceEarns = append(invoiceEarns, Earn{
			Destination: r.Public(),
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
			Sender: sender,
			Earns:  earns,
		},
		{
			Sender:  sender,
			Channel: (*PrivateKey)(&channel),
			Earns:   earns,
		},
		{
			Sender: sender,
			Earns:  invoiceEarns,
		},
	}

	for _, b := range batches {
		var initSeq int64
		if b.Channel == nil {
			info, err := env.internal.GetStellarAccountInfo(context.Background(), b.Sender.Public())
			require.NoError(t, err)
			initSeq = info.SequenceNumber
		} else {
			info, err := env.internal.GetStellarAccountInfo(context.Background(), b.Channel.Public())
			require.NoError(t, err)
			initSeq = info.SequenceNumber
		}

		result, err := env.client.SubmitEarnBatch(context.Background(), b)
		assert.NoError(t, err)

		func() {
			env.server.mu.Lock()
			defer env.server.mu.Unlock()
			defer func() { env.server.blockchains[version.KinVersion3].submits = nil }()

			assert.Len(t, env.server.blockchains[version.KinVersion3].submits, 3)
			assert.Len(t, result.Succeeded, len(earnAccounts))
			assert.Empty(t, result.Failed)

			txHashes := make(map[string]struct{})
			for i, r := range result.Succeeded {
				txHashes[string(r.TxID)] = struct{}{}
				assert.NoError(t, r.Error)
				assert.Equal(t, b.Earns[i], r.Earn)
			}
			assert.Len(t, txHashes, 3)

			for i, s := range env.server.blockchains[version.KinVersion3].submits {
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
				if b.Channel != nil {
					assert.Len(t, envelope.Signatures, 2)
					assert.EqualValues(t, b.Channel.Public(), sourceAccount[:])
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
	result, err := env.client.SubmitEarnBatch(ctx, batches[0])
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
	env.server.blockchains[version.KinVersion3].submitResponses = []*transactionpb.SubmitTransactionResponse{
		nil,
		{
			Result:    transactionpb.SubmitTransactionResponse_FAILED,
			Hash:      nil, // w/e
			ResultXdr: resultBytes,
		},
	}
	env.server.mu.Unlock()

	result, err = env.client.SubmitEarnBatch(context.Background(), batches[0])
	assert.Error(t, err)
	assert.Len(t, result.Succeeded, 100)
	assert.Len(t, result.Failed, 102)
	for i := 0; i < len(result.Succeeded); i++ {
		assert.NoError(t, result.Succeeded[i].Error)
		assert.NotNil(t, result.Succeeded[i].TxID)
		assert.Equal(t, batches[0].Earns[i], result.Succeeded[i].Earn)
	}
	for i := 0; i < 100; i++ {
		assert.Equal(t, ErrInvalidSignature, result.Failed[i].Error)
		assert.Equal(t, batches[0].Earns[100+i], result.Failed[i].Earn)
	}
	for i := 100; i < 102; i++ {
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
	env.server.blockchains[version.KinVersion3].submitResponses = []*transactionpb.SubmitTransactionResponse{
		nil,
		{
			Result:    transactionpb.SubmitTransactionResponse_FAILED,
			Hash:      nil,
			ResultXdr: resultBytes,
		},
	}
	env.server.mu.Unlock()

	result, err = env.client.SubmitEarnBatch(context.Background(), batches[0])
	assert.Error(t, err)
	assert.Len(t, result.Succeeded, 100)
	assert.Len(t, result.Failed, 102)
	for i := 0; i < len(result.Succeeded); i++ {
		assert.NoError(t, result.Succeeded[i].Error)
		assert.NotNil(t, result.Succeeded[i].TxID)
		assert.Equal(t, batches[0].Earns[i], result.Succeeded[i].Earn)
	}
	for i := 0; i < 100; i++ {
		assert.EqualValues(t, ErrInsufficientBalance, result.Failed[i].Error)
		assert.Equal(t, batches[0].Earns[100+i], result.Failed[i].Earn)
	}
	for i := 100; i < 102; i++ {
		assert.NoError(t, result.Failed[i].Error)
		assert.Equal(t, batches[0].Earns[100+i], result.Failed[i].Earn)
	}
}

func TestClient_SubmitEarnBatchDuplicateSigners(t *testing.T) {
	sender, err := NewPrivateKey()
	require.NoError(t, err)

	env, cleanup := setup(t, WithWhitelister(sender))
	defer cleanup()

	earnAccounts := make([]PrivateKey, 202)
	for i := 0; i < len(earnAccounts); i++ {
		dest, err := NewPrivateKey()
		require.NoError(t, err)
		earnAccounts[i] = dest
	}

	for _, acc := range append([]PrivateKey{sender}, earnAccounts...) {
		require.NoError(t, env.client.CreateAccount(context.Background(), acc))
	}

	var earns []Earn
	for i, r := range earnAccounts {
		earns = append(earns, Earn{
			Destination: r.Public(),
			Quarks:      int64(i) + 1,
		})
	}

	b := EarnBatch{
		Sender: sender,
		Earns:  earns,
	}

	var initSeq int64
	if b.Channel == nil {
		info, err := env.internal.GetStellarAccountInfo(context.Background(), b.Sender.Public())
		require.NoError(t, err)
		initSeq = info.SequenceNumber
	} else {
		info, err := env.internal.GetStellarAccountInfo(context.Background(), b.Channel.Public())
		require.NoError(t, err)
		initSeq = info.SequenceNumber
	}

	result, err := env.client.SubmitEarnBatch(context.Background(), b)
	assert.NoError(t, err)

	env.server.mu.Lock()
	defer env.server.mu.Unlock()
	defer func() { env.server.blockchains[version.KinVersion3].submits = nil }()

	assert.Len(t, env.server.blockchains[version.KinVersion3].submits, 3)
	assert.Len(t, result.Succeeded, len(earnAccounts))
	assert.Empty(t, result.Failed)

	txHashes := make(map[string]struct{})
	for i, r := range result.Succeeded {
		txHashes[string(r.TxID)] = struct{}{}
		assert.NoError(t, r.Error)
		assert.Equal(t, b.Earns[i], r.Earn)
	}
	assert.Len(t, txHashes, 3)

	for i, s := range env.server.blockchains[version.KinVersion3].submits {
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
		assert.EqualValues(t, b.Sender.Public(), sourceAccount[:])

		// There should only be one signature despite channel + whitelister being set
		assert.Len(t, envelope.Signatures, 1)

		assert.Equal(t, xdr.MemoTypeMemoHash, envelope.Tx.Memo.Type)

		memo, ok := kin.MemoFromXDR(envelope.Tx.Memo, true)
		assert.True(t, ok)

		assert.EqualValues(t, 1, memo.AppIndex())
		assert.EqualValues(t, make([]byte, 29), memo.ForeignKey())
	}
}

func TestClient_CreateAccountInvalidKinVersion(t *testing.T) {
	env, cleanup := setup(t, WithKinVersion(1))
	defer cleanup()

	priv, err := NewPrivateKey()
	require.NoError(t, err)

	err = env.client.CreateAccount(context.Background(), priv)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "version")
}

func TestClient_SubmitPaymentInvalidKinVersion(t *testing.T) {
	env, cleanup := setup(t, WithKinVersion(1))
	defer cleanup()

	sender, err := NewPrivateKey()
	require.NoError(t, err)
	dest, err := NewPrivateKey()
	require.NoError(t, err)

	txID, err := env.client.SubmitPayment(context.Background(), Payment{
		Sender:      sender,
		Destination: dest.Public(),
		Type:        kin.TransactionTypeNone,
		Quarks:      1,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "version")
	assert.Nil(t, txID)
}

func TestClient_Kin4AccountManagement(t *testing.T) {
	env, cleanup := setup(t, WithKinVersion(4))
	defer cleanup()

	setServiceConfigResp(t, env.v4Server, true)

	priv, err := NewPrivateKey()
	require.NoError(t, err)

	tokenAcc, _ := generateTokenAccount(ed25519.PrivateKey(priv))

	balance, err := env.client.GetBalance(context.Background(), PublicKey(tokenAcc))
	assert.Equal(t, ErrAccountDoesNotExist, err)
	assert.Zero(t, balance)

	err = env.client.CreateAccount(context.Background(), priv)
	assert.NoError(t, err)

	err = env.client.CreateAccount(context.Background(), priv)
	assert.Equal(t, ErrAccountExists, err)

	balance, err = env.client.GetBalance(context.Background(), PublicKey(tokenAcc))
	assert.NoError(t, err)
	assert.EqualValues(t, 10, balance)

	// Test resolution options
	balance, err = env.client.GetBalance(context.Background(), priv.Public(), WithAccountResolution(AccountResolutionExact))
	assert.Equal(t, ErrAccountDoesNotExist, err)
	assert.Zero(t, balance)

	balance, err = env.client.GetBalance(context.Background(), priv.Public())
	require.NoError(t, err)
	assert.EqualValues(t, 10, balance)
}

func TestClient_Kin4SubmitPayment(t *testing.T) {
	env, cleanup := setup(t, WithKinVersion(4))
	defer cleanup()

	sender, err := NewPrivateKey()
	require.NoError(t, err)
	dest, err := NewPrivateKey()
	require.NoError(t, err)

	setServiceConfigResp(t, env.v4Server, true)

	for _, acc := range [][]byte{sender, dest} {
		require.NoError(t, env.client.CreateAccount(context.Background(), acc))
	}

	payments := []Payment{
		{
			Sender:      sender,
			Destination: dest.Public(),
			Type:        kin.TransactionTypeSpend,
			Quarks:      11,
		},
		{
			Sender:      sender,
			Destination: dest.Public(),
			Type:        kin.TransactionTypeSpend,
			Quarks:      11,
			Memo:        "1-test",
		},
		{
			Sender:      sender,
			Destination: dest.Public(),
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
		txID, err := env.client.SubmitPayment(context.Background(), p)
		assert.NotNil(t, txID)
		assert.NoError(t, err)

		func() {
			env.v4Server.Mux.Lock()
			defer env.v4Server.Mux.Unlock()
			defer func() { env.v4Server.Submits = nil }()

			assert.Len(t, env.v4Server.Submits, 1)

			tx := solana.Transaction{}
			assert.NoError(t, tx.Unmarshal(env.v4Server.Submits[0].Transaction.Value))
			assert.Len(t, tx.Signatures, 2)
			assert.EqualValues(t, make([]byte, ed25519.SignatureSize), tx.Signatures[0][:])
			assert.True(t, ed25519.Verify(ed25519.PublicKey(sender.Public()), tx.Message.Marshal(), tx.Signatures[1][:]))

			assert.Len(t, tx.Message.Instructions, 2)

			if p.Memo != "" {
				memoInstr, err := memo.DecompileMemo(tx.Message, 0)
				require.NoError(t, err)
				assert.Equal(t, p.Memo, string(memoInstr.Data))
			} else if p.Invoice != nil {
				invoiceList := &commonpb.InvoiceList{
					Invoices: []*commonpb.Invoice{p.Invoice},
				}
				ilBytes, err := proto.Marshal(invoiceList)
				require.NoError(t, err)
				ilHash := sha256.Sum224(ilBytes)

				memoInstruction, err := memo.DecompileMemo(tx.Message, 0)
				require.NoError(t, err)

				m, err := kin.MemoFromBase64String(string(memoInstruction.Data), true)
				require.NoError(t, err)

				assert.Equal(t, kin.TransactionTypeSpend, m.TransactionType())
				assert.EqualValues(t, 1, m.AppIndex())
				assert.EqualValues(t, ilHash[:], m.ForeignKey()[:28])
				assert.True(t, proto.Equal(invoiceList, env.v4Server.Submits[0].InvoiceList))
			} else {
				memoInstr, err := memo.DecompileMemo(tx.Message, 0)
				require.NoError(t, err)

				m, err := kin.MemoFromBase64String(string(memoInstr.Data), true)
				require.NoError(t, err)

				assert.Equal(t, kin.TransactionTypeSpend, m.TransactionType())
				assert.EqualValues(t, 1, m.AppIndex())
				assert.EqualValues(t, make([]byte, 29), m.ForeignKey())
			}

			transferInstr, err := token.DecompileTransferAccount(tx.Message, 1)
			require.NoError(t, err)

			assert.EqualValues(t, sender.Public(), transferInstr.Source)
			assert.EqualValues(t, dest.Public(), transferInstr.Destination)
			assert.EqualValues(t, sender.Public(), transferInstr.Owner)
			assert.EqualValues(t, p.Quarks, transferInstr.Amount)
		}()
	}

	env.v4Server.Mux.Lock()
	env.v4Server.SubmitResponses = []*transactionpbv4.SubmitTransactionResponse{
		{
			Result: transactionpbv4.SubmitTransactionResponse_INVOICE_ERROR,
			InvoiceErrors: []*commonpb.InvoiceError{
				{
					Invoice: payments[2].Invoice,
					Reason:  commonpb.InvoiceError_ALREADY_PAID,
				},
			},
		},
		{
			Result: transactionpbv4.SubmitTransactionResponse_INVOICE_ERROR,
			InvoiceErrors: []*commonpb.InvoiceError{
				{
					Invoice: payments[2].Invoice,
					Reason:  commonpb.InvoiceError_WRONG_DESTINATION,
				},
			},
		},
		{
			Result: transactionpbv4.SubmitTransactionResponse_INVOICE_ERROR,
			InvoiceErrors: []*commonpb.InvoiceError{
				{
					Invoice: payments[2].Invoice,
					Reason:  commonpb.InvoiceError_SKU_NOT_FOUND,
				},
			},
		},
	}
	env.v4Server.Mux.Unlock()

	for _, e := range []error{ErrAlreadyPaid, ErrWrongDestination, ErrSKUNotFound} {
		txID, err := env.client.SubmitPayment(context.Background(), payments[2])
		assert.NotNil(t, txID)
		assert.Equal(t, e, err)
	}

	env.v4Server.Mux.Lock()
	env.v4Server.SubmitResponses = []*transactionpbv4.SubmitTransactionResponse{
		{
			Result: transactionpbv4.SubmitTransactionResponse_FAILED,
			TransactionError: &commonpbv4.TransactionError{
				Reason: commonpbv4.TransactionError_UNAUTHORIZED,
				Raw:    []byte("rawerror"),
			},
		},
	}
	env.v4Server.Mux.Unlock()

	txID, err := env.client.SubmitPayment(context.Background(), payments[2])
	assert.NotNil(t, txID)
	assert.Equal(t, ErrInvalidSignature, err)
}

func TestClient_Kin4SubmitPaymentNoServiceSubsidizer(t *testing.T) {
	env, cleanup := setup(t, WithKinVersion(4))
	defer cleanup()

	sender, err := NewPrivateKey()
	require.NoError(t, err)
	dest, err := NewPrivateKey()
	require.NoError(t, err)
	appSubsidizer, err := NewPrivateKey()
	require.NoError(t, err)

	setServiceConfigResp(t, env.v4Server, true)
	for _, acc := range [][]byte{sender, dest, appSubsidizer} {
		require.NoError(t, env.client.CreateAccount(context.Background(), acc))
	}

	env.internal.serviceConfig = nil // reset cache
	setServiceConfigResp(t, env.v4Server, false)

	p := Payment{
		Sender:      sender,
		Destination: dest.Public(),
		Type:        kin.TransactionTypeSpend,
		Quarks:      11,
	}

	txID, err := env.client.SubmitPayment(context.Background(), p)
	assert.Equal(t, ErrNoSubsidizer, err)
	assert.Nil(t, txID)

	txID, err = env.client.SubmitPayment(context.Background(), p, WithSubsidizer(appSubsidizer))
	require.NoError(t, err)
	require.NotNil(t, txID)

	env.v4Server.Mux.Lock()
	defer env.v4Server.Mux.Unlock()

	assert.Len(t, env.v4Server.Submits, 1)

	tx := solana.Transaction{}
	assert.NoError(t, tx.Unmarshal(env.v4Server.Submits[0].Transaction.Value))
	assert.Len(t, tx.Signatures, 2)
	assert.True(t, ed25519.Verify(ed25519.PublicKey(appSubsidizer.Public()), tx.Message.Marshal(), tx.Signatures[0][:]))
	assert.True(t, ed25519.Verify(ed25519.PublicKey(sender.Public()), tx.Message.Marshal(), tx.Signatures[1][:]))

	assert.Len(t, tx.Message.Instructions, 2)

	memoInstr, err := memo.DecompileMemo(tx.Message, 0)
	require.NoError(t, err)

	m, err := kin.MemoFromBase64String(string(memoInstr.Data), true)
	require.NoError(t, err)

	assert.Equal(t, kin.TransactionTypeSpend, m.TransactionType())
	assert.EqualValues(t, 1, m.AppIndex())
	assert.EqualValues(t, make([]byte, 29), m.ForeignKey())

	transferInstr, err := token.DecompileTransferAccount(tx.Message, 1)
	require.NoError(t, err)

	assert.EqualValues(t, sender.Public(), transferInstr.Source)
	assert.EqualValues(t, dest.Public(), transferInstr.Destination)
	assert.EqualValues(t, sender.Public(), transferInstr.Owner)
	assert.EqualValues(t, p.Quarks, transferInstr.Amount)
}

func TestClient_Kin4SubmitPaymentKin4AccountResolution(t *testing.T) {
	env, cleanup := setup(t, WithKinVersion(4))
	defer cleanup()

	sender, err := NewPrivateKey()
	require.NoError(t, err)
	dest, err := NewPrivateKey()
	require.NoError(t, err)
	resolvedSender, _ := generateTokenAccount(ed25519.PrivateKey(sender))
	resolvedDest, _ := generateTokenAccount(ed25519.PrivateKey(dest))

	setServiceConfigResp(t, env.v4Server, true)
	for _, acc := range [][]byte{sender, dest} {
		require.NoError(t, env.client.CreateAccount(context.Background(), acc))
	}

	p := Payment{
		Sender:      sender,
		Destination: dest.Public(),
		Type:        kin.TransactionTypeSpend,
		Quarks:      11,
	}

	// Test Preferred Account Resolution
	env.v4Server.Mux.Lock()
	env.v4Server.SubmitResponses = []*transactionpbv4.SubmitTransactionResponse{
		{
			Result: transactionpbv4.SubmitTransactionResponse_FAILED,
			TransactionError: &commonpbv4.TransactionError{
				Reason: commonpbv4.TransactionError_INVALID_ACCOUNT,
				Raw:    []byte("rawerror"),
			},
		},
	}
	env.v4Server.Mux.Unlock()

	txID, err := env.client.SubmitPayment(context.Background(), p, WithAccountResolution(AccountResolutionPreferred), WithDestResolution(AccountResolutionPreferred))
	require.NoError(t, err)
	assert.NotNil(t, txID)

	env.v4Server.Mux.Lock()
	assert.Len(t, env.v4Server.Submits, 2)
	for i, submit := range env.v4Server.Submits {
		tx := solana.Transaction{}
		assert.NoError(t, tx.Unmarshal(submit.Transaction.Value))
		assert.Len(t, tx.Signatures, 2)
		assert.EqualValues(t, make([]byte, ed25519.SignatureSize), tx.Signatures[0][:])
		assert.True(t, ed25519.Verify(ed25519.PublicKey(sender.Public()), tx.Message.Marshal(), tx.Signatures[1][:]))

		assert.Len(t, tx.Message.Instructions, 2)

		memoInstr, err := memo.DecompileMemo(tx.Message, 0)
		require.NoError(t, err)

		m, err := kin.MemoFromBase64String(string(memoInstr.Data), true)
		require.NoError(t, err)

		assert.Equal(t, kin.TransactionTypeSpend, m.TransactionType())
		assert.EqualValues(t, 1, m.AppIndex())
		assert.EqualValues(t, make([]byte, 29), m.ForeignKey())

		transferInstr, err := token.DecompileTransferAccount(tx.Message, 1)
		require.NoError(t, err)

		if i == 0 {
			assert.EqualValues(t, sender.Public(), transferInstr.Source)
			assert.EqualValues(t, dest.Public(), transferInstr.Destination)
		} else {
			assert.EqualValues(t, resolvedSender, transferInstr.Source)
			assert.EqualValues(t, resolvedDest, transferInstr.Destination)
		}
		assert.EqualValues(t, sender.Public(), transferInstr.Owner)
		assert.EqualValues(t, p.Quarks, transferInstr.Amount)
	}
	env.v4Server.Mux.Unlock()

	// Test Exact Account Resolution
	env.v4Server.Mux.Lock()
	env.v4Server.SubmitResponses = []*transactionpbv4.SubmitTransactionResponse{
		{
			Result: transactionpbv4.SubmitTransactionResponse_FAILED,
			TransactionError: &commonpbv4.TransactionError{
				Reason: commonpbv4.TransactionError_INVALID_ACCOUNT,
				Raw:    []byte("rawerror"),
			},
		},
	}
	env.v4Server.Mux.Unlock()

	txID, err = env.client.SubmitPayment(context.Background(), p, WithAccountResolution(AccountResolutionExact), WithDestResolution(AccountResolutionExact))
	assert.EqualValues(t, ErrAccountDoesNotExist, err)
	assert.NotNil(t, txID)
}

func TestClient_Kin4SubmitEarnBatch(t *testing.T) {
	env, cleanup := setup(t, WithKinVersion(4))
	defer cleanup()

	sender, err := NewPrivateKey()
	require.NoError(t, err)

	earnAccounts := make([]PrivateKey, 60)
	for i := 0; i < len(earnAccounts); i++ {
		dest, err := NewPrivateKey()
		require.NoError(t, err)
		earnAccounts[i] = dest
	}

	setServiceConfigResp(t, env.v4Server, true)

	for _, acc := range append([]PrivateKey{sender}, earnAccounts...) {
		require.NoError(t, env.client.CreateAccount(context.Background(), acc))
	}

	var earns []Earn
	for i, r := range earnAccounts {
		earns = append(earns, Earn{
			Destination: r.Public(),
			Quarks:      int64(i) + 1,
		})
	}
	var invoiceEarns []Earn
	for i, r := range earnAccounts {
		invoiceEarns = append(invoiceEarns, Earn{
			Destination: r.Public(),
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
			Sender: sender,
			Earns:  earns,
		},
		{
			Sender: sender,
			Earns:  earns,
			Memo:   "somememo",
		},
		{
			Sender: sender,
			Earns:  invoiceEarns,
		},
	}

	for _, b := range batches {
		result, err := env.client.SubmitEarnBatch(context.Background(), b)
		assert.NoError(t, err)

		func() {
			env.v4Server.Mux.Lock()
			defer env.v4Server.Mux.Unlock()
			defer func() { env.v4Server.Submits = nil }()

			assert.Len(t, env.v4Server.Submits, 4)
			assert.Len(t, result.Succeeded, len(earnAccounts))
			assert.Empty(t, result.Failed)

			txIDs := make(map[string]struct{})
			for i, r := range result.Succeeded {
				txIDs[string(r.TxID)] = struct{}{}
				assert.NoError(t, r.Error)
				assert.Equal(t, b.Earns[i], r.Earn)
			}
			assert.Len(t, txIDs, 4)

			for i, s := range env.v4Server.Submits {
				tx := solana.Transaction{}
				assert.NoError(t, tx.Unmarshal(s.Transaction.Value))
				assert.Len(t, tx.Signatures, 2)
				assert.EqualValues(t, make([]byte, ed25519.SignatureSize), tx.Signatures[0][:])
				assert.True(t, ed25519.Verify(ed25519.PublicKey(sender.Public()), tx.Message.Marshal(), tx.Signatures[1][:]))

				var batchSize int
				if b.Memo != "" {
					batchSize = 19
					memoInstr, err := memo.DecompileMemo(tx.Message, 0)
					require.NoError(t, err)
					assert.Equal(t, b.Memo, string(memoInstr.Data))
				} else if b.Earns[0].Invoice != nil {
					batchSize = 18
					invoiceList := &commonpb.InvoiceList{
						Invoices: make([]*commonpb.Invoice, 0, batchSize),
					}

					start := i * batchSize
					end := int(math.Min(float64((i+1)*batchSize), float64(len(b.Earns))))
					for j := start; j < end; j++ {
						invoiceList.Invoices = append(invoiceList.Invoices, b.Earns[j].Invoice)
					}

					ilBytes, err := proto.Marshal(invoiceList)
					require.NoError(t, err)
					ilHash := sha256.Sum224(ilBytes)

					memoInstruction, err := memo.DecompileMemo(tx.Message, 0)
					require.NoError(t, err)

					m, err := kin.MemoFromBase64String(string(memoInstruction.Data), true)
					require.NoError(t, err)

					assert.Equal(t, kin.TransactionTypeEarn, m.TransactionType())
					assert.EqualValues(t, 1, m.AppIndex())
					assert.EqualValues(t, ilHash[:], m.ForeignKey()[:28])
					assert.True(t, proto.Equal(invoiceList, s.InvoiceList))
				} else {
					batchSize = 18

					memoInstr, err := memo.DecompileMemo(tx.Message, 0)
					require.NoError(t, err)

					m, err := kin.MemoFromBase64String(string(memoInstr.Data), true)
					require.NoError(t, err)

					assert.Equal(t, kin.TransactionTypeEarn, m.TransactionType())
					assert.EqualValues(t, 1, m.AppIndex())
					assert.EqualValues(t, make([]byte, 29), m.ForeignKey())
				}

				var reqBatchSize int
				if i == len(env.v4Server.Submits)-1 {
					reqBatchSize = 60 % batchSize
				} else {
					reqBatchSize = batchSize
				}

				assert.Len(t, tx.Message.Instructions, reqBatchSize+1)
				for j := 0; j < reqBatchSize; j++ {
					transferInstr, err := token.DecompileTransferAccount(tx.Message, j+1)
					require.NoError(t, err)

					earn := b.Earns[i*batchSize+j]
					assert.EqualValues(t, sender.Public(), transferInstr.Source)
					assert.EqualValues(t, earn.Destination, transferInstr.Destination)
					assert.EqualValues(t, sender.Public(), transferInstr.Owner)
					assert.EqualValues(t, earn.Quarks, transferInstr.Amount)
				}
			}
		}()
	}

	// Ensure context cancellation works correctly.
	ctx, cancelFunc := context.WithCancel(context.Background())
	cancelFunc()
	result, err := env.client.SubmitEarnBatch(ctx, batches[0])
	assert.Error(t, err)
	assert.Empty(t, result.Succeeded, len(batches[0].Earns))
	assert.Len(t, result.Failed, len(batches[0].Earns))

	// Ensure that error handling for the following cases is correct:
	//   - Partial Success: (some of the batches succeeded)
	//   - Op Failures: operation failures are reported
	//   - Tx Failures: transaction level failures
	env.v4Server.Mux.Lock()
	env.v4Server.SubmitResponses = []*transactionpbv4.SubmitTransactionResponse{
		nil,
		{
			Result: transactionpbv4.SubmitTransactionResponse_FAILED,
			TransactionError: &commonpbv4.TransactionError{
				Reason:           commonpbv4.TransactionError_UNAUTHORIZED,
				InstructionIndex: 0,
				Raw:              []byte("rawerror"),
			},
		},
	}
	env.v4Server.Mux.Unlock()

	result, err = env.client.SubmitEarnBatch(context.Background(), batches[0])
	assert.Error(t, err)
	assert.Len(t, result.Succeeded, 18)
	assert.Len(t, result.Failed, 42)
	for i := 0; i < len(result.Succeeded); i++ {
		assert.NoError(t, result.Succeeded[i].Error)
		assert.NotNil(t, result.Succeeded[i].TxID)
		assert.Equal(t, batches[0].Earns[i], result.Succeeded[i].Earn)
	}
	for i := 0; i < 18; i++ {
		assert.Equal(t, ErrInvalidSignature, result.Failed[i].Error)
		assert.Equal(t, batches[0].Earns[18+i], result.Failed[i].Earn)
	}
	for i := 18; i < 42; i++ {
		assert.NoError(t, result.Failed[i].Error)
		assert.Equal(t, batches[0].Earns[18+i], result.Failed[i].Earn)
	}
}

func TestClient_Kin4SubmitEarnBatchNoServiceSubsidizer(t *testing.T) {
	env, cleanup := setup(t, WithKinVersion(4))
	defer cleanup()

	sender, err := NewPrivateKey()
	require.NoError(t, err)
	appSubsidizer, err := NewPrivateKey()
	require.NoError(t, err)

	earns := make([]Earn, 20)
	earnAccounts := make([]PrivateKey, 20)
	for i := 0; i < len(earnAccounts); i++ {
		dest, err := NewPrivateKey()
		require.NoError(t, err)
		earnAccounts[i] = dest

		earns[i] = Earn{
			Destination: dest.Public(),
			Quarks:      int64(i) + 1,
		}
	}

	setServiceConfigResp(t, env.v4Server, true)
	for _, acc := range append([]PrivateKey{sender, appSubsidizer}, earnAccounts...) {
		require.NoError(t, env.client.CreateAccount(context.Background(), acc))
	}

	env.internal.serviceConfig = nil // reset cache
	setServiceConfigResp(t, env.v4Server, false)

	b := EarnBatch{
		Sender: sender,
		Earns:  earns,
	}

	result, err := env.client.SubmitEarnBatch(context.Background(), b)
	assert.Equal(t, ErrNoSubsidizer, err)
	assert.Equal(t, EarnBatchResult{}, result)

	result, err = env.client.SubmitEarnBatch(context.Background(), b, WithSubsidizer(appSubsidizer))
	assert.NoError(t, err)

	env.v4Server.Mux.Lock()
	defer env.v4Server.Mux.Unlock()

	assert.Len(t, env.v4Server.Submits, 2)
	assert.Len(t, result.Succeeded, len(earnAccounts))
	assert.Empty(t, result.Failed)

	txIDs := make(map[string]struct{})
	for i, r := range result.Succeeded {
		txIDs[string(r.TxID)] = struct{}{}
		assert.NoError(t, r.Error)
		assert.Equal(t, b.Earns[i], r.Earn)
	}
	assert.Len(t, txIDs, 2)

	for i, s := range env.v4Server.Submits {
		tx := solana.Transaction{}
		assert.NoError(t, tx.Unmarshal(s.Transaction.Value))
		assert.Len(t, tx.Signatures, 2)
		assert.True(t, ed25519.Verify(ed25519.PublicKey(appSubsidizer.Public()), tx.Message.Marshal(), tx.Signatures[0][:]))
		assert.True(t, ed25519.Verify(ed25519.PublicKey(sender.Public()), tx.Message.Marshal(), tx.Signatures[1][:]))

		memoInstr, err := memo.DecompileMemo(tx.Message, 0)
		require.NoError(t, err)

		m, err := kin.MemoFromBase64String(string(memoInstr.Data), true)
		require.NoError(t, err)

		assert.Equal(t, kin.TransactionTypeEarn, m.TransactionType())
		assert.EqualValues(t, 1, m.AppIndex())
		assert.EqualValues(t, make([]byte, 29), m.ForeignKey())

		batchSize := 18
		var reqBatchSize int
		if i == len(env.v4Server.Submits)-1 {
			reqBatchSize = 20 % batchSize
		} else {
			reqBatchSize = batchSize
		}

		assert.Len(t, tx.Message.Instructions, reqBatchSize+1)
		for j := 0; j < reqBatchSize; j++ {
			transferInstr, err := token.DecompileTransferAccount(tx.Message, j+1)
			require.NoError(t, err)

			earn := b.Earns[i*batchSize+j]
			assert.EqualValues(t, sender.Public(), transferInstr.Source)
			assert.EqualValues(t, earn.Destination, transferInstr.Destination)
			assert.EqualValues(t, sender.Public(), transferInstr.Owner)
			assert.EqualValues(t, earn.Quarks, transferInstr.Amount)
		}
	}
}

func TestClient_Kin4SubmitEarnBatchAccountResolution(t *testing.T) {
	env, cleanup := setup(t, WithKinVersion(4))
	defer cleanup()

	sender, err := NewPrivateKey()
	require.NoError(t, err)
	resolvedSender, _ := generateTokenAccount(ed25519.PrivateKey(sender))
	require.NoError(t, err)

	// Test Preferred Account Resolution
	env.v4Server.Mux.Lock()
	env.v4Server.SubmitResponses = []*transactionpbv4.SubmitTransactionResponse{
		{
			Result: transactionpbv4.SubmitTransactionResponse_FAILED,
			TransactionError: &commonpbv4.TransactionError{
				Reason: commonpbv4.TransactionError_INVALID_ACCOUNT,
				Raw:    []byte("rawerror"),
			},
		},
	}
	env.v4Server.Mux.Unlock()

	earns := make([]Earn, 20)
	earnAccounts := make([]PrivateKey, 20)
	resolvedEarnAccounts := make([]PublicKey, 20)
	for i := 0; i < len(earnAccounts); i++ {
		dest, err := NewPrivateKey()
		require.NoError(t, err)
		earnAccounts[i] = dest

		earns[i] = Earn{
			Destination: dest.Public(),
			Quarks:      int64(i) + 1,
		}

		resolvedDest, _ := generateTokenAccount(ed25519.PrivateKey(dest))
		require.NoError(t, err)
		resolvedEarnAccounts[i] = PublicKey(resolvedDest)
	}

	setServiceConfigResp(t, env.v4Server, true)
	for _, acc := range append([]PrivateKey{sender}, earnAccounts...) {
		require.NoError(t, env.client.CreateAccount(context.Background(), acc))
	}
	b := EarnBatch{
		Sender: sender,
		Earns:  earns,
	}

	result, err := env.client.SubmitEarnBatch(context.Background(), b, WithAccountResolution(AccountResolutionPreferred), WithDestResolution(AccountResolutionPreferred))
	assert.NoError(t, err)

	env.v4Server.Mux.Lock()

	assert.Len(t, env.v4Server.Submits, 3)
	assert.Len(t, result.Succeeded, len(earnAccounts))
	assert.Empty(t, result.Failed)

	txIDs := make(map[string]struct{})
	for i, r := range result.Succeeded {
		txIDs[string(r.TxID)] = struct{}{}
		assert.NoError(t, r.Error)
		assert.Equal(t, b.Earns[i], r.Earn)
	}
	assert.Len(t, txIDs, 2)

	for i, s := range env.v4Server.Submits {
		batchIndex := int(math.Floor(float64(i) / 2))
		resolved := (i % 2) == 1

		tx := solana.Transaction{}
		assert.NoError(t, tx.Unmarshal(s.Transaction.Value))
		assert.Len(t, tx.Signatures, 2)
		assert.EqualValues(t, make([]byte, ed25519.SignatureSize), tx.Signatures[0][:])
		assert.True(t, ed25519.Verify(ed25519.PublicKey(sender.Public()), tx.Message.Marshal(), tx.Signatures[1][:]))

		memoInstr, err := memo.DecompileMemo(tx.Message, 0)
		require.NoError(t, err)

		m, err := kin.MemoFromBase64String(string(memoInstr.Data), true)
		require.NoError(t, err)

		assert.Equal(t, kin.TransactionTypeEarn, m.TransactionType())
		assert.EqualValues(t, 1, m.AppIndex())
		assert.EqualValues(t, make([]byte, 29), m.ForeignKey())

		batchSize := 18
		var reqBatchSize int
		if i == len(env.v4Server.Submits)-1 {
			reqBatchSize = 20 % batchSize
		} else {
			reqBatchSize = batchSize
		}

		assert.Len(t, tx.Message.Instructions, reqBatchSize+1)
		for j := 0; j < reqBatchSize; j++ {
			transferInstr, err := token.DecompileTransferAccount(tx.Message, j+1)
			require.NoError(t, err)

			earn := b.Earns[batchIndex*batchSize+j]
			if resolved {
				require.EqualValues(t, resolvedSender, transferInstr.Source)
				require.EqualValues(t, resolvedEarnAccounts[batchIndex*batchSize+j], transferInstr.Destination)
			} else {
				require.EqualValues(t, sender.Public(), transferInstr.Source)
				require.EqualValues(t, earnAccounts[batchIndex*batchSize+j].Public(), transferInstr.Destination)
			}
			require.EqualValues(t, sender.Public(), transferInstr.Owner)
			require.EqualValues(t, earn.Quarks, transferInstr.Amount)
		}
	}
	env.v4Server.Mux.Unlock()

	// Test Exact Account Resolution
	env.v4Server.Mux.Lock()
	env.v4Server.Submits = nil
	env.v4Server.SubmitResponses = []*transactionpbv4.SubmitTransactionResponse{
		{
			Result: transactionpbv4.SubmitTransactionResponse_FAILED,
			TransactionError: &commonpbv4.TransactionError{
				Reason: commonpbv4.TransactionError_INVALID_ACCOUNT,
				Raw:    []byte("rawerror"),
			},
		},
	}
	env.v4Server.Mux.Unlock()

	result, err = env.client.SubmitEarnBatch(context.Background(), b, WithAccountResolution(AccountResolutionExact), WithDestResolution(AccountResolutionExact))
	assert.Equal(t, err, ErrAccountDoesNotExist)

	env.v4Server.Mux.Lock()
	defer env.v4Server.Mux.Unlock()

	assert.Len(t, env.v4Server.Submits, 1)
	assert.Empty(t, result.Succeeded)
	assert.Len(t, result.Failed, len(earnAccounts))
	for i := 0; i < 18; i++ {
		assert.Equal(t, ErrAccountDoesNotExist, result.Failed[i].Error)
		assert.Equal(t, earns[i], result.Failed[i].Earn)
	}
	for i := 18; i < 20; i++ {
		assert.NoError(t, result.Failed[i].Error)
		assert.Equal(t, earns[i], result.Failed[i].Earn)
	}
}

func TestClient_CreateAccountMigration(t *testing.T) {
	env, cleanup := setup(t, WithDesiredKinVersion(4))
	defer cleanup()

	priv, err := NewPrivateKey()
	require.NoError(t, err)

	setServiceConfigResp(t, env.v4Server, true)

	err = env.client.CreateAccount(context.Background(), priv)
	require.NoError(t, err)

	env.v4Server.Mux.Lock()
	assert.Len(t, env.v4Server.Accounts, 1)

	tokenAcc, _ := generateTokenAccount(ed25519.PrivateKey(priv))
	assert.NotNil(t, env.v4Server.Accounts[PublicKey(tokenAcc).Base58()])
	env.v4Server.Mux.Unlock()
}

func TestClient_GetBalanceMigration(t *testing.T) {
	env, cleanup := setup(t, WithDesiredKinVersion(4))
	defer cleanup()

	priv, err := NewPrivateKey()
	require.NoError(t, err)

	env.v4Server.Mux.Lock()
	env.v4Server.Accounts = map[string]*accountpbv4.AccountInfo{
		priv.Public().Base58(): {
			AccountId: &commonpbv4.SolanaAccountId{Value: priv.Public()},
			Balance:   10,
		},
	}
	env.v4Server.Mux.Unlock()

	balance, err := env.client.GetBalance(context.Background(), priv.Public())
	assert.NoError(t, err)
	assert.EqualValues(t, 10, balance)
}

func TestClient_SubmitPaymentMigration(t *testing.T) {
	env, cleanup := setup(t, WithDesiredKinVersion(4))
	defer cleanup()

	sender, err := NewPrivateKey()
	require.NoError(t, err)
	dest, err := NewPrivateKey()
	require.NoError(t, err)

	setServiceConfigResp(t, env.v4Server, true)

	p := Payment{
		Sender:      sender,
		Destination: dest.Public(),
		Type:        kin.TransactionTypeSpend,
		Quarks:      11,
	}

	txID, err := env.client.SubmitPayment(context.Background(), p)
	require.NoError(t, err)
	require.NotNil(t, txID)

	env.v4Server.Mux.Lock()
	defer env.v4Server.Mux.Unlock()

	assert.Len(t, env.v4Server.Submits, 1)

	tx := solana.Transaction{}
	assert.NoError(t, tx.Unmarshal(env.v4Server.Submits[0].Transaction.Value))
	assert.Len(t, tx.Signatures, 2)
	assert.EqualValues(t, make([]byte, ed25519.SignatureSize), tx.Signatures[0][:])
	assert.True(t, ed25519.Verify(ed25519.PublicKey(sender.Public()), tx.Message.Marshal(), tx.Signatures[1][:]))

	assert.Len(t, tx.Message.Instructions, 2)

	memoInstr, err := memo.DecompileMemo(tx.Message, 0)
	require.NoError(t, err)

	m, err := kin.MemoFromBase64String(string(memoInstr.Data), true)
	require.NoError(t, err)

	assert.Equal(t, kin.TransactionTypeSpend, m.TransactionType())
	assert.EqualValues(t, 1, m.AppIndex())
	assert.EqualValues(t, make([]byte, 29), m.ForeignKey())

	transferInstr, err := token.DecompileTransferAccount(tx.Message, 1)
	require.NoError(t, err)

	assert.EqualValues(t, sender.Public(), transferInstr.Source)
	assert.EqualValues(t, dest.Public(), transferInstr.Destination)
	assert.EqualValues(t, sender.Public(), transferInstr.Owner)
	assert.EqualValues(t, p.Quarks, transferInstr.Amount)
}

func TestClient_EstimateEarnBatchTxSize(t *testing.T) {
	subsidizer, err := NewPrivateKey()
	require.NoError(t, err)
	owner, err := NewPrivateKey()
	require.NoError(t, err)
	earns := make([]Earn, 20)
	for i := 1; i < 20; i++ {
		dest, err := NewPrivateKey()
		require.NoError(t, err)
		earns[i] = Earn{Destination: dest.Public(), Quarks: int64(i + 1)}
	}
	earns[0] = earns[1] // test a duplicate destination

	for _, tc := range []struct {
		hasSeparateSender bool
		hasAgoraMemo      bool
		hasTextMemo       bool
	}{
		{false, false, false},
		{false, false, true},
		{false, true, false},
		{true, false, false},
		{true, false, true},
		{true, true, false},
	} {
		var transferSender PrivateKey
		var m kin.Memo
		var textMemo string
		if tc.hasSeparateSender {
			transferSender, err = NewPrivateKey()
			require.NoError(t, err)
		}
		if tc.hasAgoraMemo {
			m, err = kin.NewMemo(1, kin.TransactionTypeSpend, 1, []byte("somefk"))
			require.NoError(t, err)
		}
		if tc.hasTextMemo {
			textMemo = "t-test"
		}

		for i := 1; i < len(earns); i++ {
			batch := earns[:i]
			var instructions []solana.Instruction
			if tc.hasAgoraMemo {
				instructions = append(instructions, memo.Instruction(base64.StdEncoding.EncodeToString(m[:])))
			} else if tc.hasTextMemo {
				instructions = append(instructions, memo.Instruction(textMemo))
			}

			if transferSender == nil {
				transferSender = owner
			}
			for _, earn := range batch {
				instructions = append(
					instructions,
					token.Transfer(
						ed25519.PublicKey(transferSender.Public()),
						ed25519.PublicKey(earn.Destination),
						ed25519.PublicKey(owner.Public()),
						uint64(earn.Quarks),
					),
				)
			}
			tx := solana.NewTransaction(ed25519.PublicKey(subsidizer.Public()), instructions...)
			require.NoError(t, tx.Sign(ed25519.PrivateKey(subsidizer), ed25519.PrivateKey(owner)))

			estimated := estimateEarnBatchTxSize(batch, tc.hasSeparateSender, tc.hasAgoraMemo, textMemo)
			require.Equal(t, len(tx.Marshal()), estimated)
		}
	}
}
