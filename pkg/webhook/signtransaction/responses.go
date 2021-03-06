package signtransaction

import (
	"bytes"

	"github.com/kinecosystem/go/xdr"
	"github.com/pkg/errors"
)

const (
	AlreadyPaid      Reason = "already_paid"
	WrongDestination Reason = "wrong_destination"
	SKUNotFound      Reason = "sku_not_found"
)

// SuccessResponse represents a 200 OK response to a sign transaction request.
type SuccessResponse struct {
	// EnvelopeXDR is a base64-encoded transaction envelope XDR
	EnvelopeXDR []byte `json:"envelope_xdr"`
}

// ForbiddenResponse represents a 403 Forbidden response to a sign transaction request.
type ForbiddenResponse struct {
	Message       string         `json:"message"`
	InvoiceErrors []InvoiceError `json:"invoice_errors"`
}

// InvoiceError is an error specific to an operation (or its corresponding invoice) in the transaction
type InvoiceError struct {
	OperationIndex uint32 `json:"operation_index"`
	Reason         Reason `json:"reason"`
}

// Reason indicates why a transaction operation was rejected
type Reason string

func (r *SuccessResponse) GetEnvelopeXDR() (*xdr.TransactionEnvelope, error) {
	if len(r.EnvelopeXDR) == 0 {
		return nil, errors.New("envelope_xdr cannot have length of 0")
	}

	e := &xdr.TransactionEnvelope{}
	if _, err := xdr.Unmarshal(bytes.NewBuffer(r.EnvelopeXDR), e); err != nil {
		return nil, errors.New("envelope_xdr was not a valid transaction envelope")
	}

	return e, nil
}
