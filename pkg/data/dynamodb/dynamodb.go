package dynamodb

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbiface"
	"github.com/pkg/errors"

	"github.com/kinecosystem/kin-api/genproto/common/v3"

	"github.com/kinecosystem/agora-common/env"
	"github.com/kinecosystem/agora-transaction-services/pkg/data"
)

const (
	baseTableName = "transaction-data"
	putCondition  = "attribute_not_exists(prefix)"
)

var (
	putConditionStr = aws.String(putCondition)
)

type db struct {
	db dynamodbiface.ClientAPI

	tableName *string
}

// New returns a dynamo backed data.Store
func New(e env.AgoraEnvironment, client dynamodbiface.ClientAPI) (data.Store, error) {
	if !e.IsValid() {
		return nil, errors.Errorf("invalid environment: %s", e)
	}

	return &db{
		db:        client,
		tableName: aws.String(fmt.Sprintf("%s-%s", baseTableName, e)),
	}, nil
}

// Add implements data.Store.Add.
func (d *db) Add(ctx context.Context, ad *common.AgoraData) error {
	item, err := marshalData(ad)
	if err != nil {
		return err
	}

	_, err = d.db.PutItemRequest(&dynamodb.PutItemInput{
		TableName:           d.tableName,
		Item:                item,
		ConditionExpression: putConditionStr,
	}).Send(ctx)
	if err != nil {
		if aErr, ok := err.(awserr.Error); ok {
			switch aErr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				return data.ErrCollision
			}
		}

		return errors.Wrapf(err, "failed to persist message")
	}

	return nil
}

// Get implements data.Store.Get.
func (d *db) Get(ctx context.Context, prefixOrKey []byte) (*common.AgoraData, error) {
	queryExpression := "prefix = :prefix"
	queryValues := make(map[string]dynamodb.AttributeValue)

	switch len(prefixOrKey) {
	case 32: // full key
		queryExpression += " and suffix = :suffix"
		queryValues[":suffix"] = dynamodb.AttributeValue{B: prefixOrKey[29:]}
		fallthrough
	case 29: // prefix
		queryValues[":prefix"] = dynamodb.AttributeValue{B: prefixOrKey[:29]}
	default:
		return nil, errors.Errorf("invalid key len: %d", len(prefixOrKey))
	}

	// Note: we don't need to page here because we're limiting the results to 2.
	//
	// todo: expand the set of results and validate against the entire memo input.
	//       this should be currently protected against via the Add(), though.
	resp, err := d.db.QueryRequest(&dynamodb.QueryInput{
		TableName:                 d.tableName,
		Limit:                     aws.Int64(2),
		KeyConditionExpression:    aws.String(queryExpression),
		ExpressionAttributeValues: queryValues,
	}).Send(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to dynamo")
	}

	if len(resp.Items) == 0 {
		return nil, data.ErrNotFound
	}
	if len(resp.Items) > 1 {
		// todo: address this issue
		return nil, data.ErrCollision
	}

	return unmarshalData(resp.Items[0])
}
