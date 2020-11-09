package events

import (
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"
)

// Event is a top level event container for blockchain events.
type Event struct {
	TransactionEvent *TransactionEvent `json:"transaction_event"`
}

// TransactionEvent is an event containing transaction details.
type TransactionEvent struct {
	KinVersion  int                   `json:"kin_version"`
	TxHash      []byte                `json:"tx_hash"`
	TxID        []byte                `json:"tx_id"`
	InvoiceList *commonpb.InvoiceList `json:"invoice_list"`

	StellarEvent *StellarEvent `json:"stellar_event"`
	SolanaEvent  *SolanaEvent  `json:"solana_event"`
}

// StellarEvent is stellar specific data related to
// a transaction.
type StellarEvent struct {
	EnvelopeXDR []byte `json:"envelope_xdr"`
	ResultXDR   []byte `json:"result_xdr"`
}

// SolanaEvent is stellar specific data related to
// a transaction.
type SolanaEvent struct {
	Transaction         []byte `json:"transaction"`
	TransactionError    string `json:"transaction_error"`
	TransactionErrorRaw []byte `json:"transaction_error_raw"`
}
