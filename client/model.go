package client

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/memo"
	"github.com/kinecosystem/go/xdr"
	"github.com/pkg/errors"

	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"
	transactionpbv4 "github.com/kinecosystem/agora-api/genproto/transaction/v4"

	"github.com/kinecosystem/agora/pkg/version"
)

// KinToQuarks converts a string representation of kin
// the quark value.
//
// An error is returned if the value string is invalid, or
// it cannot be accurately represented as quarks. For example,
// a value smaller than quarks, or a value _far_ greater than
// the supply.
func KinToQuarks(val string) (int64, error) {
	parts := strings.Split(val, ".")
	if len(parts) > 2 {
		return 0, errors.New("invalid kin value")
	}

	if len(parts[0]) > 14 {
		return 0, errors.New("value cannot be represented")
	}

	kin, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, err
	}

	var quarks uint64
	if len(parts) == 2 {
		if len(parts[1]) > 5 {
			return 0, errors.New("value cannot be represented")
		}

		quarks, err = strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return 0, errors.Wrap(err, "invalid decimal component")
		}
	}

	return kin*1e5 + int64(quarks), nil
}

// MustKinToQuarks calls KinToQuarks, panicking if there's an error.
//
// This should only be used if you know for sure this will not panic.
func MustKinToQuarks(val string) int64 {
	result, err := KinToQuarks(val)
	if err != nil {
		panic(err)
	}

	return result
}

// QuarksToKin converts an int64 amount of quarks to the
// string representation of kin.
func QuarksToKin(amount int64) string {
	if amount < 1e5 {
		return fmt.Sprintf("0.%05d", amount)
	}

	return fmt.Sprintf("%d.%05d", amount/1e5, amount%1e5)
}

// Payment represents a kin payment.
type Payment struct {
	Sender      PrivateKey
	Destination PublicKey
	Type        kin.TransactionType
	Quarks      int64

	Channel *PrivateKey

	Invoice *commonpb.Invoice
	Memo    string
}

// ReadOnlyPayment represents a kin payment, where
// none of the private keys are known.
type ReadOnlyPayment struct {
	Sender      PublicKey
	Destination PublicKey
	Type        kin.TransactionType
	Quarks      int64

	Invoice *commonpb.Invoice
	Memo    string
}

func parsePaymentsFromEnvelope(envelope xdr.TransactionEnvelope, txType kin.TransactionType, invoiceList *commonpb.InvoiceList, v version.KinVersion) ([]ReadOnlyPayment, error) {
	payments := make([]ReadOnlyPayment, 0, len(envelope.Tx.Operations))

	if invoiceList != nil && len(invoiceList.Invoices) != len(envelope.Tx.Operations) {
		return nil, errors.Errorf(
			"provided invoice count (%d) does not match op count (%d)",
			len(invoiceList.Invoices),
			len(envelope.Tx.Operations),
		)
	}

	for i, op := range envelope.Tx.Operations {
		// Currently we only support payment operations in this RPC.
		//
		// We could potentially expand this to CreateAccount functions,
		// as well as merge account. However, GetTransaction() is primarily
		// only used for payments.
		if op.Body.PaymentOp == nil {
			continue
		}

		var source xdr.AccountId
		if op.SourceAccount != nil {
			source = *op.SourceAccount
		} else {
			source = envelope.Tx.SourceAccount
		}

		sender, err := publicKeyFromStellarXDR(source)
		if err != nil {
			return payments, errors.Wrap(err, "invalid sender account")
		}
		dest, err := publicKeyFromStellarXDR(op.Body.PaymentOp.Destination)
		if err != nil {
			return payments, errors.Wrap(err, "invalid destination account")
		}

		var quarks int64
		if v == version.KinVersion2 {
			if op.Body.PaymentOp.Asset.Type != xdr.AssetTypeAssetTypeCreditAlphanum4 || op.Body.PaymentOp.Asset.AlphaNum4.AssetCode != kinAssetCode {
				// Only Kin payment operations are supported in this RPC.
				continue
			}

			// On Kin 2, the smallest denomination is 1e-7, unlike on Kin 3, where the smallest amount (a quark) is 1e-5.
			// We must therefore convert the payment amount from the base currency to the equivalent amount in quarks
			// accordingly.
			quarks = int64(op.Body.PaymentOp.Amount / 100)
		} else {
			quarks = int64(op.Body.PaymentOp.Amount)
		}

		p := ReadOnlyPayment{
			Sender:      sender,
			Destination: dest,
			Quarks:      quarks,
			Type:        txType,
		}

		if invoiceList != nil {
			// This indexing is 'safe', as agora validates on ingestion that
			// the amount of operations in a transaction matches the amount
			// of invoices submitted, such that there is a direct mapping
			// between the transaction Operations and the InvoiceList.
			//
			// Additionally, we check they're the same above as an extra
			// safety measure.
			p.Invoice = invoiceList.Invoices[i]
		} else if envelope.Tx.Memo.Text != nil {
			p.Memo = *envelope.Tx.Memo.Text
		}

		payments = append(payments, p)
	}

	return payments, nil
}

func parsePaymentsFromProto(item *transactionpbv4.HistoryItem) ([]ReadOnlyPayment, error) {
	if item.InvoiceList != nil && len(item.InvoiceList.Invoices) != len(item.Payments) {
		return nil, errors.Errorf(
			"provided invoice count (%d) does not match payment count (%d)",
			len(item.InvoiceList.Invoices),
			len(item.Payments),
		)
	}

	var textMemo string
	var txType kin.TransactionType

	switch t := item.RawTransaction.(type) {
	case *transactionpbv4.HistoryItem_SolanaTransaction:
		tx := &solana.Transaction{}
		err := tx.Unmarshal(t.SolanaTransaction.Value)
		if err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal test transaction")
		}

		if bytes.Equal(tx.Message.Accounts[tx.Message.Instructions[0].ProgramIndex], memo.ProgramKey) {
			m, err := memo.DecompileMemo(tx.Message, 0)
			if err != nil {
				return nil, errors.Wrap(err, "failed to decompile memo instruction")
			}
			decoded := [32]byte{}
			_, err = base64.StdEncoding.Decode(decoded[:], m.Data)
			if err == nil && kin.IsValidMemoStrict(decoded) {
				txType = kin.Memo(decoded).TransactionType()
			} else {
				textMemo = string(m.Data)
			}
		}
	case *transactionpbv4.HistoryItem_StellarTransaction:
		var envelope xdr.TransactionEnvelope
		if err := envelope.UnmarshalBinary(t.StellarTransaction.EnvelopeXdr); err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal xdr")
		}

		kinMemo, ok := kin.MemoFromXDR(envelope.Tx.Memo, true)
		if ok {
			txType = kinMemo.TransactionType()
		} else if envelope.Tx.Memo.Text != nil {
			textMemo = *envelope.Tx.Memo.Text
		}
	}

	payments := make([]ReadOnlyPayment, len(item.Payments))
	for i, payment := range item.Payments {
		p := ReadOnlyPayment{
			Sender:      payment.Source.Value,
			Destination: payment.Destination.Value,
			Type:        txType,
			Quarks:      payment.Amount,
		}
		if item.InvoiceList != nil {
			p.Invoice = item.InvoiceList.Invoices[i]
		} else if textMemo != "" {
			p.Memo = textMemo
		}
		payments[i] = p
	}

	return payments, nil

}

// TransactionData contains high level metadata and payments
// contained in a transaction.
type TransactionData struct {
	TxID     []byte
	TxState  TransactionState
	Payments []ReadOnlyPayment
	Errors   TransactionErrors
}

type TransactionState int

const (
	TransactionStateUnknown TransactionState = iota
	TransactionStateSuccess
	TransactionStateFailed
	TransactionStatePending
)

func txStateFromProto(state transactionpbv4.GetTransactionResponse_State) TransactionState {
	switch state {
	case transactionpbv4.GetTransactionResponse_SUCCESS:
		return TransactionStateSuccess
	case transactionpbv4.GetTransactionResponse_FAILED:
		return TransactionStateFailed
	case transactionpbv4.GetTransactionResponse_PENDING:
		return TransactionStatePending
	default:
		return TransactionStateUnknown
	}
}

// EarnBatch is a batch of Earn payments coming from a single
// sender/source.
type EarnBatch struct {
	Sender  PrivateKey
	Channel *PrivateKey

	Memo string

	Earns []Earn
}

// Earn represents a earn payment in an earn batch.
type Earn struct {
	Destination PublicKey
	Quarks      int64
	Invoice     *commonpb.Invoice
}

// EarnBatchResult contains the result of an EarnBatch.
//
// All earns are contained in the union of {Succeeded, Failed}.
type EarnBatchResult struct {
	Succeeded []EarnResult
	Failed    []EarnResult
}

// EarnResult contains the result of a single earn within an
// earn batch.
type EarnResult struct {
	TxHash []byte
	Earn   Earn
	Error  error
}
