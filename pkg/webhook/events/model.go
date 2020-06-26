package events

import (
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"
)

// Event is a top level event container for blockchain events.
type Event struct {
	TransactionEvent TransactionEvent `json:"transaction_event"`
}

// TransactionEvent is an event containing transaction details.
type TransactionEvent struct {
	KinVersion  int                   `json:"kin_version"`
	TxHash      []byte                `json:"tx_hash"`
	InvoiceList *commonpb.InvoiceList `json:"invoice_list"`

	StellarEvent *StellarData `json:"stellar_event"`
}

// StellarData is stellar specific data related to
// a transaction.
type StellarData struct {
	EnvelopeXDR []byte `json:"envelope_xdr"`
	ResultXDR   []byte `json:"result_xdr"`
}
