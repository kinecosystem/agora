package test

import (
	"testing"

	"github.com/kinecosystem/go/keypair"
	hProtocol "github.com/kinecosystem/go/protocols/horizon"
	"github.com/kinecosystem/go/protocols/horizon/base"
	"github.com/kinecosystem/go/strkey"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/require"
)

func GenerateHorizonAccount(accountID string, nativeBalance string, sequence string) *hProtocol.Account {
	return &hProtocol.Account{
		HistoryAccount: hProtocol.HistoryAccount{
			ID: accountID,
		},
		Balances: []hProtocol.Balance{{
			Balance: nativeBalance,
			Asset:   base.Asset{Type: "native"},
		}},
		Sequence: sequence,
	}
}

func GenerateAccountID(t *testing.T) (*keypair.Full, xdr.AccountId) {
	kp, err := keypair.Random()
	require.NoError(t, err)

	pubKey, err := strkey.Decode(strkey.VersionByteAccountID, kp.Address())
	require.NoError(t, err)
	var senderPubKey xdr.Uint256
	copy(senderPubKey[:], pubKey)

	return kp, xdr.AccountId{
		Type:    xdr.PublicKeyTypePublicKeyTypeEd25519,
		Ed25519: &senderPubKey,
	}
}

func GenerateTransactionEnvelope(src xdr.AccountId, operations []xdr.Operation) xdr.TransactionEnvelope {
	return xdr.TransactionEnvelope{
		Tx: xdr.Transaction{
			SourceAccount: src,
			Operations:    operations,
		},
	}
}

func GenerateCreateOperation(src *xdr.AccountId, dest xdr.AccountId) xdr.Operation {
	return xdr.Operation{
		SourceAccount: src,
		Body: xdr.OperationBody{
			Type:            xdr.OperationTypeCreateAccount,
			CreateAccountOp: &xdr.CreateAccountOp{Destination: dest},
		},
	}
}

func GeneratePaymentOperation(src *xdr.AccountId, dest xdr.AccountId) xdr.Operation {
	return xdr.Operation{
		SourceAccount: src,
		Body: xdr.OperationBody{
			Type:      xdr.OperationTypePayment,
			PaymentOp: &xdr.PaymentOp{Destination: dest},
		},
	}
}

func GenerateMergeOperation(src *xdr.AccountId, dest xdr.AccountId) xdr.Operation {
	return xdr.Operation{
		SourceAccount: src,
		Body: xdr.OperationBody{
			Type:        xdr.OperationTypeAccountMerge,
			Destination: &dest,
		},
	}
}

func GenerateLEC(lecType xdr.LedgerEntryChangeType, id xdr.AccountId, seqNum xdr.SequenceNumber, balance xdr.Int64) xdr.LedgerEntryChange {
	lec := xdr.LedgerEntryChange{
		Type: lecType,
	}
	entry := &xdr.LedgerEntry{
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeAccount,
			Account: &xdr.AccountEntry{
				AccountId: id,
				SeqNum:    seqNum,
				Balance:   balance,
			},
		},
	}

	switch lecType {
	case xdr.LedgerEntryChangeTypeLedgerEntryCreated:
		lec.Created = entry
	case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
		lec.Updated = entry
	case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
		lec.Removed = &xdr.LedgerKey{
			Type:    xdr.LedgerEntryTypeAccount,
			Account: &xdr.LedgerKeyAccount{AccountId: id},
		}
	case xdr.LedgerEntryChangeTypeLedgerEntryState:
		lec.State = entry
	}
	return lec
}

func GenerateTransactionMeta(v int32, operations []xdr.OperationMeta) xdr.TransactionMeta {
	m := xdr.TransactionMeta{V: v}
	switch v {
	case 1:
		m.V1 = &xdr.TransactionMetaV1{
			Operations: operations,
		}
	default:
		m.Operations = &operations
	}
	return m
}
