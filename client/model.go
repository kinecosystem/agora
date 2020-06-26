package client

import (
	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/go/xdr"
	"github.com/pkg/errors"

	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"
)

func KinToQuarks(amount float64) int64 {
	return int64(amount * 1e5)
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

	Receivers []EarnReceiver
}

// EarnReceiver represents a receiver in an earn batch.
type EarnReceiver struct {
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
	TxHash   []byte
	Receiver EarnReceiver
	Error    error
}
