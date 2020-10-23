package testutil

import (
	"crypto/ed25519"
	"encoding/base64"
	"testing"

	"github.com/golang/protobuf/ptypes"
	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/agora-common/solana"
	solanamemo "github.com/kinecosystem/agora-common/solana/memo"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/stellar/go/network"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/testutil"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

func GenerateSolanaEntry(t *testing.T, slot uint64, confirmed bool, sender ed25519.PrivateKey, receivers []ed25519.PublicKey, invoiceHash []byte, textMemo *string) (*model.Entry, []byte) {
	var instructions []solana.Instruction

	if invoiceHash != nil {
		memo, err := kin.NewMemo(1, kin.TransactionTypeSpend, 1, invoiceHash)
		require.NoError(t, err)
		instructions = append(instructions, solanamemo.Instruction(base64.StdEncoding.EncodeToString(memo[:])))
	} else if textMemo != nil {
		instructions = append(instructions, solanamemo.Instruction(*textMemo))
	}

	for i := range receivers {
		instructions = append(instructions, token.Transfer(sender.Public().(ed25519.PublicKey), receivers[i], receivers[i], uint64(slot+uint64(i)+1)))
	}

	txn := solana.NewTransaction(
		sender.Public().(ed25519.PublicKey),
		instructions...,
	)
	assert.NoError(t, txn.Sign(sender))

	return &model.Entry{
		Version: model.KinVersion_KIN4,
		Kind: &model.Entry_Solana{
			Solana: &model.SolanaEntry{
				Slot:        slot,
				Confirmed:   confirmed,
				Transaction: txn.Marshal(),
			},
		},
	}, txn.Signature()
}

// todo: options may be better for longer term testing
func GenerateStellarEntry(t *testing.T, ledger uint64, txOrder int, sender xdr.AccountId, receivers []xdr.AccountId, invoiceHash []byte, textMemo *string) (*model.Entry, []byte) {
	var ops []xdr.Operation
	for _, r := range receivers {
		ops = append(ops, testutil.GeneratePaymentOperation(&sender, r))
	}
	envelope := testutil.GenerateTransactionEnvelope(sender, int(ledger)+txOrder, ops)

	if invoiceHash != nil {
		memo, err := kin.NewMemo(1, kin.TransactionTypeSpend, 1, invoiceHash)
		require.NoError(t, err)

		envelope.Tx.Memo = xdr.Memo{
			Type: xdr.MemoTypeMemoHash,
			Hash: &xdr.Hash{},
		}
		copy(envelope.Tx.Memo.Hash[:], memo[:])
	} else if textMemo != nil {
		envelope.Tx.Memo = xdr.Memo{
			Type: xdr.MemoTypeMemoText,
			Text: textMemo,
		}
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
