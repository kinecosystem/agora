package dynamodb

import (
	"context"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbiface"
	"github.com/kinecosystem/agora-common/aws/dynamodb/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/kinecosystem/agora/pkg/app"
)

type db struct {
	log *logrus.Entry
	db  dynamodbiface.ClientAPI
}

// New returns a dynamodb-backed app.ConfigStore
func New(client dynamodbiface.ClientAPI) app.ConfigStore {
	return &db{
		log: logrus.StandardLogger().WithField("type", "app/dynamodb"),
		db:  client,
	}
}

// Add implements app.ConfigStore.Add
func (d *db) Add(ctx context.Context, appIndex uint16, config *app.Config) error {
	item, err := toItem(appIndex, config)
	if err != nil {
		return err
	}

	_, err = d.db.PutItemRequest(&dynamodb.PutItemInput{
		TableName:           tableNameStr,
		Item:                item,
		ConditionExpression: putConditionStr,
	}).Send(ctx)
	if err != nil {
		if util.IsConditionalCheckFailed(err) {
			return app.ErrExists
		}

		return errors.Wrap(err, "failed to store app config")
	}

	return nil
}

// Get implements app.ConfigStore.Get
func (d *db) Get(ctx context.Context, appIndex uint16) (*app.Config, error) {
	resp, err := d.db.GetItemRequest(&dynamodb.GetItemInput{
		TableName: tableNameStr,
		Key: map[string]dynamodb.AttributeValue{
			tableHashKey: {
				N: aws.String(strconv.Itoa(int(appIndex))),
			},
		},
	}).Send(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get app config")
	}

	if len(resp.Item) == 0 {
		return nil, app.ErrNotFound
	}

	return fromItem(resp.Item)
}
