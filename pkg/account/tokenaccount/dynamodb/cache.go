package dynamodb

import (
	"context"
	"crypto/ed25519"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbiface"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/kinecosystem/agora/pkg/account/tokenaccount"
)

type cache struct {
	log    *logrus.Entry
	client dynamodbiface.ClientAPI

	itemTTL time.Duration
}

// New returns a dynamodb-backed solana.Cache
func New(client dynamodbiface.ClientAPI, itemTTL time.Duration) tokenaccount.Cache {
	return &cache{
		log:     logrus.StandardLogger().WithField("type", "app/dynamodb"),
		client:  client,
		itemTTL: itemTTL,
	}
}

// Get implements solana.Cache.Add
func (c *cache) Put(ctx context.Context, owner ed25519.PublicKey, tokenAccounts []ed25519.PublicKey) error {
	item, err := toItem(owner, tokenAccounts, time.Now().Add(c.itemTTL))
	if err != nil {
		return err
	}

	_, err = c.client.PutItemRequest(&dynamodb.PutItemInput{
		TableName: tableNameStr,
		Item:      item,
	}).Send(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to store token accounts")
	}

	return nil
}

// Get implements solana.Cache.Get
func (c *cache) Get(ctx context.Context, owner ed25519.PublicKey) ([]ed25519.PublicKey, error) {
	resp, err := c.client.GetItemRequest(&dynamodb.GetItemInput{
		TableName: tableNameStr,
		Key: map[string]dynamodb.AttributeValue{
			tableHashKey: {
				B: owner,
			},
		},
	}).Send(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get token accounts")
	}

	if len(resp.Item) == 0 {
		return nil, tokenaccount.ErrTokenAccountsNotFound
	}

	_, tokenAccounts, expiry, err := fromItem(resp.Item)
	if err != nil {
		return nil, err
	}

	if expiry.Before(time.Now()) {
		return nil, tokenaccount.ErrTokenAccountsNotFound
	}

	return tokenAccounts, nil
}

func (c *cache) Delete(ctx context.Context, owner ed25519.PublicKey) error {
	_, err := c.client.DeleteItemRequest(&dynamodb.DeleteItemInput{
		TableName: tableNameStr,
		Key: map[string]dynamodb.AttributeValue{
			tableHashKey: {
				B: owner,
			},
		},
	}).Send(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to delete owner entry")
	}

	return nil
}
