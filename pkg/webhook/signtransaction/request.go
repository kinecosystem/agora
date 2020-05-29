package signtransaction

import (
	"encoding/base64"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	transactionpb "github.com/kinecosystem/agora-api/genproto/transaction/v3"

	"github.com/kinecosystem/agora/pkg/webhook/common"
)

// RequestBody contains the body of a sign transaction request.
type RequestBody struct {
	EnvelopeXDR common.EnvelopeXDR `json:"envelope_xdr"`
	InvoiceList common.InvoiceList `json:"invoice_list,omitempty"`
}

func RequestBodyFromProto(req *transactionpb.SubmitTransactionRequest) (*RequestBody, error) {
	reqBody := &RequestBody{
		EnvelopeXDR: common.EnvelopeXDR(base64.StdEncoding.EncodeToString(req.EnvelopeXdr)),
	}

	if req.InvoiceList != nil {
		b, err := proto.Marshal(req.InvoiceList)
		if err != nil {
			return nil, errors.New("failed to marshal invoice list")
		}
		reqBody.InvoiceList = common.InvoiceList(base64.StdEncoding.EncodeToString(b))
	}

	return reqBody, nil
}
