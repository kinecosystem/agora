package dynamodb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbiface"
	"github.com/golang/protobuf/proto"
	dynamodbutil "github.com/kinecosystem/agora-common/aws/dynamodb/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"

	"github.com/kinecosystem/agora/pkg/invoice"
)

const (
	tableName       = "tx_invoices"
	putCondition    = "attribute_not_exists(tx_hash)"
	hashKey         = "tx_hash"
	invoiceListAttr = "invoice_list"
)

var (
	tableNameStr    = aws.String(tableName)
	putConditionStr = aws.String(putCondition)
)

type db struct {
	log *logrus.Entry
	db  dynamodbiface.ClientAPI
}

// New returns a dynamo-backed invoice.Store
func New(client dynamodbiface.ClientAPI) invoice.Store {
	return &db{
		log: logrus.StandardLogger().WithField("type", "invoice/dynamodb"),
		db:  client,
	}
}

// Put implements invoice.Store.Put.
func (d *db) Put(ctx context.Context, txHash []byte, il *commonpb.InvoiceList) error {
	if len(txHash) != 32 {
		return errors.New("txHash not 32 bytes")
	}

	ilBytes, err := proto.Marshal(il)
	if err != nil {
		return errors.Wrap(err, "failed to marshal invoice list")
	}

	_, err = d.db.PutItemRequest(&dynamodb.PutItemInput{
		TableName: tableNameStr,
		Item: map[string]dynamodb.AttributeValue{
			hashKey:         {B: txHash},
			invoiceListAttr: {B: ilBytes},
		},
		ConditionExpression: putConditionStr,
	}).Send(ctx)
	if err != nil {
		if dynamodbutil.IsConditionalCheckFailed(err) {
			return invoice.ErrExists
		}

		return errors.Wrapf(err, "failed to store invoice list")
	}

	return nil
}

// Get implements invoice.Store.Get.
func (d *db) Get(ctx context.Context, txHash []byte) (*commonpb.InvoiceList, error) {
	if len(txHash) != 32 {
		return nil, errors.New("txHash not 32 bytes")
	}

	resp, err := d.db.GetItemRequest(&dynamodb.GetItemInput{
		TableName: tableNameStr,
		Key: map[string]dynamodb.AttributeValue{
			hashKey: {B: txHash},
		},
	}).Send(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get invoice")
	}

	val, ok := resp.Item[invoiceListAttr]
	if !ok {
		return nil, invoice.ErrNotFound
	}

	il := &commonpb.InvoiceList{}
	if err := proto.Unmarshal(val.B, il); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal invoice list")
	}

	return il, nil
}
