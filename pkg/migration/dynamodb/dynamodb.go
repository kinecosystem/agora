package dynamodb

import (
	"context"
	"crypto/ed25519"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbiface"
	dynamodbutil "github.com/kinecosystem/agora-common/aws/dynamodb/util"
	"github.com/kinecosystem/go/strkey"
	"github.com/pkg/errors"

	"github.com/kinecosystem/agora/pkg/migration"
)

type db struct {
	client dynamodbiface.ClientAPI
}

// New returns a new dynamodb backed migration.Store
func New(client dynamodbiface.ClientAPI) migration.Store {
	return &db{
		client: client,
	}
}

// Get implements migration.Store.Get.
func (db *db) Get(ctx context.Context, account ed25519.PublicKey) (migration.State, error) {
	address, err := strkey.Encode(strkey.VersionByteAccountID, account)
	if err != nil {
		return migration.State{}, errors.Wrap(err, "failed to encode address")
	}

	resp, err := db.client.GetItemRequest(&dynamodb.GetItemInput{
		TableName:      stateTableStr,
		ConsistentRead: aws.Bool(true),
		Key: map[string]dynamodb.AttributeValue{
			stateTableHashKey: {S: aws.String(address)},
		},
	}).Send(ctx)
	if err != nil {
		return migration.State{}, err
	}

	if len(resp.Item) == 0 {
		return migration.State{}, nil
	}

	return getState(resp.Item)
}

// Update implements migration.Store.Update.
func (db *db) Update(ctx context.Context, account ed25519.PublicKey, prev migration.State, next migration.State) error {
	address, err := strkey.Encode(strkey.VersionByteAccountID, account)
	if err != nil {
		return errors.Wrap(err, "failed to encode address")
	}

	nextItem, err := getItem(address, next)
	if err != nil {
		return errors.Wrap(err, "failed to marshal next state")
	}

	// Zero state can either be represented as no entry,
	// or an entry representing zero state.
	updateExpression := statePutExpressionStr
	if prev != migration.ZeroState {
		updateExpression = stateUpdateExpressionStr
	}

	_, err = db.client.PutItemRequest(&dynamodb.PutItemInput{
		TableName:           stateTableStr,
		Item:                nextItem,
		ConditionExpression: updateExpression,
		ExpressionAttributeNames: map[string]string{
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]dynamodb.AttributeValue{
			":account":        {S: aws.String(address)},
			":prev_status":    {N: aws.String(strconv.Itoa(int(prev.Status)))},
			":prev_signature": {B: prev.Signature[:]},
		},
	}).Send(ctx)
	if err != nil {
		if dynamodbutil.IsConditionalCheckFailed(err) {
			return migration.ErrStatusMismatch
		}

		return errors.Wrap(err, "failed to update state")
	}

	return nil
}

// GetCount implements migration.Store.GetCount
func (db *db) GetCount(ctx context.Context, account ed25519.PublicKey) (int, error) {
	resp, err := db.client.GetItemRequest(&dynamodb.GetItemInput{
		TableName: requestTableStr,
		Key: map[string]dynamodb.AttributeValue{
			requestTableHashKey: {B: account},
		},
	}).Send(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get request count")
	}

	if len(resp.Item) == 0 {
		return 0, nil
	}

	return getCount(resp.Item)
}

// IncrementCount implements migration.Store.IncrementCount
func (db *db) IncrementCount(ctx context.Context, account ed25519.PublicKey) error {
	_, err := db.client.UpdateItemRequest(&dynamodb.UpdateItemInput{
		TableName:        requestTableStr,
		UpdateExpression: requestUpdateExprStr,
		Key: map[string]dynamodb.AttributeValue{
			requestTableHashKey: {B: account},
		},
		ExpressionAttributeValues: map[string]dynamodb.AttributeValue{
			":inc":   {N: aws.String("1")},
			":start": {N: aws.String("0")},
		},
		ExpressionAttributeNames: map[string]string{
			"#count": "count",
		},
	}).Send(ctx)

	if err != nil {
		return errors.Wrap(err, "failed to increment request count")
	}

	return nil
}
