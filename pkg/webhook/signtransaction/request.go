package signtransaction

import (
	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/agora-common/kin/version"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/webhook/signtransaction"
	"github.com/pkg/errors"

	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"
)

func CreateSolanaRequest(txn solana.Transaction, invoiceList *commonpb.InvoiceList) (*signtransaction.Request, error) {
	reqBody := &signtransaction.Request{
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
