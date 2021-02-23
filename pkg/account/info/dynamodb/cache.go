package dynamodb

import (
	"context"
	"crypto/ed25519"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbiface"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v4"

	"github.com/kinecosystem/agora/pkg/account/info"
)

type cache struct {
	log             *logrus.Entry
	client          dynamodbiface.ClientAPI
	itemTTL         time.Duration
	negativeItemTTL time.Duration
}

// NewCache returns a dynamodb-backed info.Cache
func NewCache(
	client dynamodbiface.ClientAPI,
	ttl time.Duration,
	negativeItemTTL time.Duration,
) info.Cache {
	return &cache{
		log:             logrus.StandardLogger().WithField("type", "app/dynamodb"),
		client:          client,
		itemTTL:         ttl,
		negativeItemTTL: negativeItemTTL,
	}
}

// Get implements info.Cache.Add
func (c *cache) Put(ctx context.Context, info *accountpb.AccountInfo) error {
	ttl := c.itemTTL
	if info.Balance < 0 {
		ttl = c.negativeItemTTL
	}

	item, err := toItem(info, time.Now().Add(ttl))
	if err != nil {
		return err
	}

	_, err = c.client.PutItemRequest(&dynamodb.PutItemInput{
		TableName: tableNameStr,
		Item:      item,
	}).Send(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to store account info")
	}

	return nil
}

// Get implements info.Cache.Get
func (c *cache) Get(ctx context.Context, key ed25519.PublicKey) (*accountpb.AccountInfo, error) {
	resp, err := c.client.GetItemRequest(&dynamodb.GetItemInput{
		TableName: tableNameStr,
		Key: map[string]dynamodb.AttributeValue{
			tableHashKey: {
				B: key,
			},
		},
	}).Send(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get account info")
	}

	if len(resp.Item) == 0 {
		return nil, info.ErrAccountInfoNotFound
	}

	accountInfo, expiry, err := fromItem(resp.Item)
	if err != nil {
		return nil, err
	}

	if expiry.Before(time.Now()) {
		return nil, info.ErrAccountInfoNotFound
	}

	return accountInfo, nil
}

func (c *cache) Del(ctx context.Context, key ed25519.PublicKey) (bool, error) {
	resp, err := c.client.DeleteItemRequest(&dynamodb.DeleteItemInput{
		TableName: tableNameStr,
		Key: map[string]dynamodb.AttributeValue{
			tableHashKey: {
				B: key,
			},
		},
		ReturnValues: dynamodb.ReturnValueAllOld,
	}).Send(ctx)
	if err != nil {
		return false, errors.Wrap(err, "failed to delete cache entry")
	}

	return len(resp.Attributes) > 0, nil
}
