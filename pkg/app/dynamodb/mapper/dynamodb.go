package dynamodb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbiface"
	dynamodbutil "github.com/kinecosystem/agora-common/aws/dynamodb/util"
	"github.com/pkg/errors"

	"github.com/kinecosystem/agora/pkg/app"
)

const (
	tableName    = "app-id-mappings"
	putCondition = "attribute_not_exists(app_id)"
	appIDKey     = "app_id"
)

var (
	tableNameStr    = aws.String(tableName)
	putConditionStr = aws.String(putCondition)
)

type mapper struct {
	client dynamodbiface.ClientAPI
}

// New returns a dynamodb-backed app.Mapper
func New(client dynamodbiface.ClientAPI) app.Mapper {
	return &mapper{
		client: client,
	}
}

// Add implements app.Mapper.Add
func (m *mapper) Add(ctx context.Context, appID string, appIndex uint16) error {
	item, err := toItem(appID, appIndex)
	if err != nil {
		return err
	}

	_, err = m.client.PutItemRequest(&dynamodb.PutItemInput{
		TableName:           tableNameStr,
		Item:                item,
		ConditionExpression: putConditionStr,
	}).Send(ctx)
	if err != nil {
		if dynamodbutil.IsConditionalCheckFailed(err) {
			return app.ErrMappingExists
		}

		return errors.Wrap(err, "failed to add app ID mapping")
	}

	return nil
}

// Add implements app.Mapper.GetAppIndex
func (m *mapper) GetAppIndex(ctx context.Context, appID string) (appIndex uint16, err error) {
	resp, err := m.client.GetItemRequest(&dynamodb.GetItemInput{
		TableName: tableNameStr,
		Key: map[string]dynamodb.AttributeValue{
			appIDKey: {
				S: aws.String(appID),
			},
		},
	}).Send(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get app index")
	}

	if len(resp.Item) == 0 {
		return 0, app.ErrMappingNotFound
	}

	appIndex, err = fromItem(resp.Item)
	if err != nil {
		return 0, err
	}

	return appIndex, nil
}
