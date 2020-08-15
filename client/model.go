package client

import (
	"math/big"

	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/go/xdr"
	"github.com/pkg/errors"

	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"
)

var (
	quarkCoeff = big.NewFloat(1e5)
)

// KinToQuarks converts a string representation of kin
// the quark value.
//
// An error is returned if the value string is invalid, or
// it cannot be accurately represented as quarks. For example,
// a value smaller than quarks, or a value _far_ greater than
// the supply.
func KinToQuarks(val string) (int64, error) {
	x, _, err := big.ParseFloat(val, 10, 64, big.ToZero)
	if err != nil {
		return 0, err
	}

	r, accuracy := new(big.Float).Mul(x, quarkCoeff).Int64()
	if accuracy != big.Exact {
		return 0, errors.New("value cannot be represented with quarks")
	}

	return r, nil
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
	quarks := big.NewFloat(0)
	quarks.SetInt64(amount)

	return new(big.Float).Quo(quarks, quarkCoeff).Text('f', 5)
}

// Payment represents a kin payment.
type Payment struct {
	Sender      PrivateKey
	Destination PublicKey
	Type        kin.TransactionType
	Quarks      int64

	Source *PrivateKey

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

func parsePaymentsFromEnvelope(envelope xdr.TransactionEnvelope, txType kin.TransactionType, invoiceList *commonpb.InvoiceList) ([]ReadOnlyPayment, error) {
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

		p := ReadOnlyPayment{
			Sender:      sender,
			Destination: dest,
			Quarks:      int64(op.Body.PaymentOp.Amount),
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

// TransactionData contains high level metadata and payments
// contained in a transaction.
type TransactionData struct {
	TxHash   []byte
	Payments []ReadOnlyPayment
	Errors   TransactionErrors
}

// EarnBatch is a batch of Earn payments coming from a single
// sender/source.
type EarnBatch struct {
	Sender PrivateKey
	Source *PrivateKey

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
