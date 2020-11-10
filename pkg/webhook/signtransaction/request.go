package signtransaction

import (
	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/pkg/errors"

	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"
	transactionpb "github.com/kinecosystem/agora-api/genproto/transaction/v3"

	"github.com/kinecosystem/agora/pkg/version"
)

// RequestBody contains the body of a sign transaction request.
type RequestBody struct {
	KinVersion int `json:"kin_version"`
	// EnvelopeXDR is a base64-encoded transaction envelope XDR
	EnvelopeXDR []byte `json:"envelope_xdr"`
	// SolanaTransaction is a base64-encoded Solana transaction
	SolanaTransaction []byte `json:"solana_transaction"`
	// InvoiceList is a base64-encoded protobuf InvoiceList
	InvoiceList []byte `json:"invoice_list,omitempty"`
}

func CreateStellarRequest(v version.KinVersion, req *transactionpb.SubmitTransactionRequest) (*RequestBody, error) {
	reqBody := &RequestBody{
		KinVersion:  int(v),
		EnvelopeXDR: req.EnvelopeXdr,
	}

	if req.InvoiceList != nil {
		b, err := proto.Marshal(req.InvoiceList)
		if err != nil {
			return nil, errors.New("failed to marshal invoice list")
		}
		reqBody.InvoiceList = b
	}

	return reqBody, nil
}

func CreateSolanaRequest(txn solana.Transaction, invoiceList *commonpb.InvoiceList) (*RequestBody, error) {
	reqBody := &RequestBody{
		KinVersion:        int(version.KinVersion4),
		SolanaTransaction: txn.Marshal(),
	}

	if invoiceList != nil {
		b, err := proto.Marshal(invoiceList)
		if err != nil {
			return nil, errors.New("failed to marshal invoice list")
		}
		reqBody.InvoiceList = b
	}

	return reqBody, nil
}
