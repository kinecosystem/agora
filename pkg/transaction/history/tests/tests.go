package tests

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/testutil"
	"github.com/kinecosystem/agora/pkg/transaction/history"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

func RunTests(t *testing.T, rw history.ReaderWriter, teardown func()) {
	for _, tf := range []func(t *testing.T, rw history.ReaderWriter){
		testRoundTrip_Stellar,
		testOrdering,
	} {
		tf(t, rw)
		teardown()
	}
}

func testRoundTrip_Stellar(t *testing.T, rw history.ReaderWriter) {
	t.Run("TestRoundTrip_Stellar", func(t *testing.T) {
		ctx := context.Background()
		accounts := generateAccounts(t, 10)
		entry, hash := generateEntry(t, 1, accounts[0], accounts[1:])

		// Assert no previous state
		_, err := rw.GetTransaction(ctx, hash)
		assert.Equal(t, history.ErrNotFound, err)
		for i := 1; i < len(accounts); i++ {
			entries, err := rw.GetAccountTransactions(ctx, accounts[i].Address(), nil, nil)
			assert.NoError(t, err)
			assert.Empty(t, entries)
		}

		require.NoError(t, rw.Write(ctx, entry))

		actual, err := rw.GetTransaction(ctx, hash)
		require.NoError(t, err)
		assert.True(t, proto.Equal(actual, entry))

		for i := 1; i < len(accounts); i++ {
			entries, err := rw.GetAccountTransactions(ctx, accounts[i].Address(), nil, nil)
			assert.NoError(t, err)
			require.Len(t, entries, 1)
			require.True(t, proto.Equal(actual, entries[0]))
		}
	})
}

func testOrdering(t *testing.T, rw history.ReaderWriter) {
	t.Run("TestOrdering", func(t *testing.T) {
		ctx := context.Background()
		accounts := generateAccounts(t, 10)

		generated := make([]*model.Entry, 100)
		for i := 0; i < 100; i++ {
			entry, _ := generateEntry(t, uint64(i), accounts[0], accounts[1:])
			generated[i] = entry

			require.NoError(t, rw.Write(ctx, entry))
		}

		for i := 1; i < len(accounts); i++ {
			entries, err := rw.GetAccountTransactions(ctx, accounts[i].Address(), nil, nil)
			assert.NoError(t, err)
			require.Len(t, entries, 100)

			for e := 0; e < 100; e++ {
				require.True(t, proto.Equal(generated[e], entries[e]))
			}
		}
	})
}

func generateAccounts(t *testing.T, n int) []xdr.AccountId {
	accounts := make([]xdr.AccountId, n)
	for i := 0; i < n; i++ {
		_, accountID := testutil.GenerateAccountID(t)
		accounts[i] = accountID
	}
	return accounts
}

func generateEntry(t *testing.T, ledger uint64, sender xdr.AccountId, receivers []xdr.AccountId) (*model.Entry, []byte) {
	var ops []xdr.Operation
	for _, r := range receivers {
		ops = append(ops, testutil.GeneratePaymentOperation(&sender, r))
	}
	envelope := testutil.GenerateTransactionEnvelope(sender, ops)

	txnBytes, err := envelope.Tx.MarshalBinary()
	require.NoError(t, err)
	envelopeBytes, err := envelope.MarshalBinary()
	require.NoError(t, err)

	opResults := make([]xdr.OperationResult, len(receivers))
	for i := 0; i < len(receivers); i++ {
		opResults[i].Code = xdr.OperationResultCodeOpInner
		opResults[i].Tr = &xdr.OperationResultTr{
			Type: xdr.OperationTypePayment,
			PaymentResult: &xdr.PaymentResult{
				Code: xdr.PaymentResultCodePaymentSuccess,
			},
		}
	}

	resultBytes, err := testutil.GenerateTransactionResult(xdr.TransactionResultCodeTxSuccess, opResults).MarshalBinary()
	require.NoError(t, err)

	hash := sha256.Sum256(txnBytes)

	return &model.Entry{
		Version: model.KinVersion_KIN3,
		Kind: &model.Entry_Stellar{
			Stellar: &model.StellarEntry{
				Ledger:          ledger,
				LedgerCloseTime: ptypes.TimestampNow(),
				EnvelopeXdr:     envelopeBytes,
				ResultXdr:       resultBytes,
			},
		},
	}, hash[:]
}
