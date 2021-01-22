package dynamodb

import (
	"context"
	"crypto/ed25519"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbiface"
	dynamodbutil "github.com/kinecosystem/agora-common/aws/dynamodb/util"
	"github.com/kinecosystem/agora-common/kin/version"
	"github.com/kinecosystem/go/strkey"
	"github.com/pkg/errors"

	"github.com/kinecosystem/agora/pkg/migration"
)

type db struct {
	client dynamodbiface.ClientAPI
	table  *string
}

// New returns a new dynamodb backed migration.Store
func New(client dynamodbiface.ClientAPI, v version.KinVersion) migration.Store {
	var table *string

	switch v {
	case version.KinVersion2:
		table = stateTableKin2Str
	case version.KinVersion3:
		table = stateTableKin3Str
	}

	return &db{
		client: client,
		table:  table,
	}
}

// Get implements migration.Store.Get.
func (db *db) Get(ctx context.Context, account ed25519.PublicKey) (state migration.State, exists bool, err error) {
	address, err := strkey.Encode(strkey.VersionByteAccountID, account)
	if err != nil {
		return migration.State{}, false, errors.Wrap(err, "failed to encode address")
	}

	resp, err := db.client.GetItemRequest(&dynamodb.GetItemInput{
		TableName:      db.table,
		ConsistentRead: aws.Bool(true),
		Key: map[string]dynamodb.AttributeValue{
			stateTableHashKey: {S: aws.String(address)},
		},
	}).Send(ctx)
	if err != nil {
		return migration.State{}, false, err
	}

	if len(resp.Item) == 0 {
		return migration.State{}, false, nil
	}

	state, err = getState(resp.Item)
	if err != nil {
		return migration.State{}, false, err
	}

	return state, true, err
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
		TableName:           db.table,
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
