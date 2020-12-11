package dynamodb

import (
	"crypto/ed25519"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"github.com/pkg/errors"
)

const (
	tableName = "token-accounts"

	tableHashKey = "owner"
	ttlKey       = "expiry_time"
)

var (
	tableNameStr = aws.String(tableName)
	ttlKeyStr    = aws.String(ttlKey)
)

type tokenAccountsItem struct {
	Owner         []byte   `dynamodbav:"owner"`
	TokenAccounts [][]byte `dynamodbav:"token_accounts"`
	ExpiryTime    int64    `dynamodbav:"expiry_time"`
}

func toItem(owner ed25519.PublicKey, tokenAccounts []ed25519.PublicKey, expiryTime time.Time) (map[string]dynamodb.AttributeValue, error) {
	if len(tokenAccounts) == 0 {
		return nil, errors.New("must store at least one token account")
	}

	accounts := make([][]byte, len(tokenAccounts))
	for i, tokenAccount := range tokenAccounts {
		accounts[i] = tokenAccount
	}

	return dynamodbattribute.MarshalMap(&tokenAccountsItem{
		Owner:         owner,
		TokenAccounts: accounts,
		ExpiryTime:    expiryTime.Unix(),
	})
}

func fromItem(item map[string]dynamodb.AttributeValue) (owner ed25519.PublicKey, tokenAccounts []ed25519.PublicKey, err error) {
	var accountsItem tokenAccountsItem
	if err := dynamodbattribute.UnmarshalMap(item, &accountsItem); err != nil {
		return nil, nil, errors.Wrapf(err, "failed to unmarshal token accounts")
	}

	tokenAccounts = make([]ed25519.PublicKey, len(accountsItem.TokenAccounts))
	for i, account := range accountsItem.TokenAccounts {
		tokenAccounts[i] = account
	}
	return accountsItem.Owner, tokenAccounts, nil
}
