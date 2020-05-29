package signtransaction

import (
	"bytes"
	"encoding/base64"

	"github.com/kinecosystem/go/xdr"
	"github.com/pkg/errors"

	"github.com/kinecosystem/agora/pkg/webhook/common"
)

const (
	AlreadyPaid      Reason = "already_paid"
	WrongDestination Reason = "wrong_destination"
)

// SuccessResponse represents a 200 OK response to a sign transaction request.
type SuccessResponse struct {
	EnvelopeXDR common.EnvelopeXDR `json:"envelope_xdr"`
}

// BadRequestResponse represents a 400 Bad Request response to a sign transaction request.
type BadRequestResponse struct {
	Message string `json:"message"`
}

// BadRequestResponse represents a 403 Forbidden response to a sign transaction request.
type ForbiddenResponse struct {
	Message       string         `json:"message"`
	InvoiceErrors []InvoiceError `json:"invoice_errors"`
}

// NotFoundResponse represents a 404 Not Found response to a sign trasnaction request.
type NotFoundResponse struct {
	Message string `json:"message"`
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

	envelopeBytes, err := base64.StdEncoding.DecodeString(string(r.EnvelopeXDR))
	if err != nil {
		return nil, errors.New("envelope_xdr was not base64-encoded")
	}

	e := &xdr.TransactionEnvelope{}
	if _, err := xdr.Unmarshal(bytes.NewBuffer(envelopeBytes), e); err != nil {
		return nil, errors.New("envelope_xdr was not a valid transaction envelope")
	}

	return e, nil
}
