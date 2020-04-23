package dynamodb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbiface"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	commonpb "github.com/kinecosystem/kin-api-internal/genproto/common/v3"

	"github.com/kinecosystem/agora-transaction-services-internal/pkg/invoice"
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

// Add implements invoice.Store.Add.
func (d *db) Add(ctx context.Context, inv *commonpb.Invoice, txHash []byte) error {
	item, err := toItem(inv, txHash)
	if err != nil {
		return err
	}

	_, err = d.db.PutItemRequest(&dynamodb.PutItemInput{
		TableName:           tableNameStr,
		Item:                item,
		ConditionExpression: putConditionStr,
	}).Send(ctx)
	if err != nil {
		if aErr, ok := err.(awserr.Error); ok {
			switch aErr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				return invoice.ErrExists
			}
		}

		return errors.Wrapf(err, "failed to store invoice")
	}

	return nil
}

// Get implements invoice.Store.Get.
func (d *db) Get(ctx context.Context, invoiceHash []byte) (*invoice.Record, error) {
	if len(invoiceHash) != 28 {
		return nil, errors.Errorf("invalid invoice hash len: %d", len(invoiceHash))
	}

	resp, err := d.db.GetItemRequest(&dynamodb.GetItemInput{
		TableName: tableNameStr,
		Key: map[string]dynamodb.AttributeValue{
			tableHashKey: {
				B: invoiceHash,
			},
		},
	}).Send(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get invoice")
	}

	if len(resp.Item) == 0 {
		return nil, invoice.ErrNotFound
	}
	return fromItem(resp.Item)
}
