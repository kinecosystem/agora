package dynamodb

import (
	"context"
	"crypto/ed25519"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbiface"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/pkg/errors"

	"github.com/kinecosystem/agora/pkg/account"
)

const (
	tableName = "account-owner-mapping"
	tokenKey  = "token_account"
	ownerAttr = "owner"
)

var (
	tableNameStr = aws.String(tableName)
)

type db struct {
	db dynamodbiface.ClientAPI
}

// New returns a dynamo-backed invoice.Store
func New(client dynamodbiface.ClientAPI) account.Mapper {
	return &db{
		db: client,
	}
}

func (d *db) Get(ctx context.Context, tokenAccount ed25519.PublicKey, _ solana.Commitment) (ed25519.PublicKey, error) {
	resp, err := d.db.GetItemRequest(&dynamodb.GetItemInput{
		TableName: tableNameStr,
		Key: map[string]dynamodb.AttributeValue{
			tokenKey: {B: tokenAccount},
		},
	}).Send(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to store mapping")
	}
	if len(resp.Item) == 0 {
		return nil, account.ErrNotFound
	}

	owner, ok := resp.Item[ownerAttr]
	if !ok || len(owner.B) != ed25519.PublicKeySize {
		return nil, errors.Wrap(err, "invalid mapped key")
	}

	return owner.B, nil
}

func (d *db) Add(ctx context.Context, tokenAccount, owner ed25519.PublicKey) error {
	_, err := d.db.PutItemRequest(&dynamodb.PutItemInput{
		TableName: tableNameStr,
		Item: map[string]dynamodb.AttributeValue{
			tokenKey:  {B: tokenAccount},
			ownerAttr: {B: owner},
		},
	}).Send(ctx)
	if err != nil {
		return errors.Wrapf(err, "failed to store mapping")
	}

	return nil
}
