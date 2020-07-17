package testutil

import (
	"testing"

	"github.com/golang/protobuf/ptypes"
	"github.com/kinecosystem/agora-common/kin"
	"github.com/stellar/go/network"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/testutil"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

func GenerateEntry(t *testing.T, ledger uint64, txOrder int, sender xdr.AccountId, receivers []xdr.AccountId, invoiceHash []byte) (*model.Entry, []byte) {
	var ops []xdr.Operation
	for _, r := range receivers {
		ops = append(ops, testutil.GeneratePaymentOperation(&sender, r))
	}
	envelope := testutil.GenerateTransactionEnvelope(sender, int(ledger)+txOrder, ops)

	if invoiceHash != nil {
		memo, err := kin.NewMemo(1, kin.TransactionTypeSpend, 0, invoiceHash)
		require.NoError(t, err)

		envelope.Tx.Memo = xdr.Memo{
			Type: xdr.MemoTypeMemoHash,
			Hash: &xdr.Hash{},
		}
		copy(envelope.Tx.Memo.Hash[:], memo[:])
	}

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

	hash, err := network.HashTransaction(&envelope.Tx, "network passphrase")
	require.NoError(t, err)

	// taken from: https://github.com/kinecosystem/go/blob/master/services/horizon/internal/toid/main.go
	//
	// note: the details here don't matter, just that the byte order is comparable
	pagingToken := uint64((ledger << 32)) | (uint64(txOrder) << 12)

	return &model.Entry{
		Version: model.KinVersion_KIN3,
		Kind: &model.Entry_Stellar{
			Stellar: &model.StellarEntry{
				Ledger:            ledger,
				PagingToken:       pagingToken,
				LedgerCloseTime:   ptypes.TimestampNow(),
				NetworkPassphrase: "network passphrase",
				EnvelopeXdr:       envelopeBytes,
				ResultXdr:         resultBytes,
			},
		},
	}, hash[:]
}

func GetOrderingKey(t *testing.T, e *model.Entry) []byte {
	k, err := e.GetOrderingKey()
	require.NoError(t, err)
	return k
}
