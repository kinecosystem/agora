package dynamodb

import (
	"context"
	"crypto/ed25519"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbiface"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/kinecosystem/agora/pkg/account/solana/accountinfo"
)

type store struct {
	log    *logrus.Entry
	client dynamodbiface.ClientAPI
}

// NewStore returns a dynamodb-backed state.StateStore
func NewStore(
	client dynamodbiface.ClientAPI,
) accountinfo.StateStore {
	return &store{
		log:    logrus.StandardLogger().WithField("type", "accountinfo/dynamodb"),
		client: client,
	}
}

// Put implements state.StateStore.Put
func (s *store) Put(ctx context.Context, state *accountinfo.State) error {
	item, err := toStoreItem(state)
	if err != nil {
		return err
	}

	_, err = s.client.PutItemRequest(&dynamodb.PutItemInput{
		TableName: storeTableNameStr,
		Item:      item,
	}).Send(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to store account info")
	}

	return nil
}

// Get implements state.StateStore.Get
func (s *store) Get(ctx context.Context, account ed25519.PublicKey) (*accountinfo.State, error) {
	resp, err := s.client.GetItemRequest(&dynamodb.GetItemInput{
		TableName: storeTableNameStr,
		Key: map[string]dynamodb.AttributeValue{
			storeTableHashKey: {
				B: account,
			},
		},
	}).Send(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get account info")
	}

	if len(resp.Item) == 0 {
		return nil, accountinfo.ErrNotFound
	}

	return fromStoreItem(resp.Item)
}

// Delete implements state.StateStore.Delete
func (s *store) Delete(ctx context.Context, account ed25519.PublicKey) error {
	_, err := s.client.DeleteItemRequest(&dynamodb.DeleteItemInput{
		TableName: storeTableNameStr,
		Key: map[string]dynamodb.AttributeValue{
			storeTableHashKey: {
				B: account,
			},
		},
	}).Send(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to delete account info")
	}

	return nil
}

func (s *store) GetAccountsByOwner(ctx context.Context, owner ed25519.PublicKey) (states []ed25519.PublicKey, err error) {
	resp, err := s.client.QueryRequest(&dynamodb.QueryInput{
		TableName:              storeTableNameStr,
		IndexName:              storeGSINameStr,
		KeyConditionExpression: ownerQueryStr,
		ExpressionAttributeNames: map[string]string{
			"#owner": "owner",
		},
		ExpressionAttributeValues: map[string]dynamodb.AttributeValue{
			":owner": {B: owner},
		},
	}).Send(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get accounts by owner")
	}

	if len(resp.Items) == 0 {
		return make([]ed25519.PublicKey, 0), nil
	}

	accounts := make([]ed25519.PublicKey, len(resp.Items))
	for i, item := range resp.Items {
		if attr, ok := item["account"]; ok {
			accounts[i] = attr.B
		} else {
			return nil, errors.New("item missing account attribute")
		}
	}
	return accounts, nil
}
