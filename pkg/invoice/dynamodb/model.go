package dynamodb

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	commonpb "github.com/kinecosystem/kin-api/genproto/common/v3"

	"github.com/kinecosystem/agora-transaction-services-internal/pkg/invoice"
)

const (
	tableName    = "invoices"
	putCondition = "attribute_not_exists(invoice_hash)"

	tableHashKey = "invoice_hash"
)

var (
	tableNameStr    = aws.String(tableName)
	putConditionStr = aws.String(putCondition)
)

type invoiceItem struct {
	InvoiceHash []byte `dynamodbav:"invoice_hash"`
	TxHash      []byte `dynamodbav:"tx_hash"`
	Contents    []byte `dynamodbav:"contents"`
}

func toItem(inv *commonpb.Invoice, txHash []byte) (map[string]dynamodb.AttributeValue, error) {
	if len(txHash) != 32 {
		return nil, errors.New("transaction hash must be 32 bytes")
	}

	invoiceHash, err := invoice.GetHash(inv)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get invoice hash")
	}

	b, err := proto.Marshal(inv)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal invoice")
	}

	return dynamodbattribute.MarshalMap(&invoiceItem{
		InvoiceHash: invoiceHash,
		TxHash:      txHash,
		Contents:    b,
	})
}

func fromItem(item map[string]dynamodb.AttributeValue) (*invoice.Record, error) {
	var invoiceItem invoiceItem
	if err := dynamodbattribute.UnmarshalMap(item, &invoiceItem); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal invoice item")
	}

	inv := &commonpb.Invoice{}
	if err := proto.Unmarshal(invoiceItem.Contents, inv); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal invoice")
	}

	return &invoice.Record{
		Invoice: inv,
		TxHash:  invoiceItem.TxHash,
	}, nil
}
