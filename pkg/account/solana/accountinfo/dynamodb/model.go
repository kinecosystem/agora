package dynamodb

import (
	"crypto/ed25519"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/agora/pkg/account/solana/accountinfo"
	"github.com/pkg/errors"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v4"
)

const (
	tableName    = "account-info"
	tableHashKey = "key"
	ttlKey       = "expiry_time"

	storeTableName    = "account-state"
	storeTableHashKey = "account"
	storeGSIName      = "owner-index"
	storeIndexHashKey = "owner"
	ownerQueryExpr    = "#owner = :owner"
)

var (
	tableNameStr = aws.String(tableName)
	ttlKeyStr    = aws.String(ttlKey)

	storeTableNameStr = aws.String(storeTableName)
	storeGSINameStr   = aws.String(storeGSIName)
	ownerQueryStr     = aws.String(ownerQueryExpr)
)

type infoItem struct {
	Key         []byte `dynamodbav:"key"`
	AccountInfo []byte `dynamodbav:"account_info"`
	ExpiryTime  int64  `dynamodbav:"expiry_time"`
}

type storeItem struct {
	Account []byte `dynamodbav:"account"`
	Owner   []byte `dynamodbav:"owner"`
	Balance int64  `dynamodbav:"balance"`
	Slot    uint64 `dynamodbav:"slot"`
}

func toItem(info *accountpb.AccountInfo, expiryTime time.Time) (map[string]dynamodb.AttributeValue, error) {
	if info == nil || len(info.AccountId.Value) != ed25519.PublicKeySize {
		return nil, errors.New("info must not be nil and account ID must be a valid ed25519 public key")
	}

	b, err := proto.Marshal(info)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal account info")
	}

	return dynamodbattribute.MarshalMap(&infoItem{
		Key:         info.AccountId.Value,
		AccountInfo: b,
		ExpiryTime:  expiryTime.Unix(),
	})
}

func fromItem(item map[string]dynamodb.AttributeValue) (info *accountpb.AccountInfo, expiry time.Time, err error) {
	var infoItem infoItem
	if err := dynamodbattribute.UnmarshalMap(item, &infoItem); err != nil {
		return nil, time.Time{}, errors.Wrapf(err, "failed to unmarshal account info item")
	}

	info = &accountpb.AccountInfo{}
	if err = proto.Unmarshal(infoItem.AccountInfo, info); err != nil {
		return nil, time.Time{}, errors.Wrap(err, "failed to unmarshal account info")
	}
	return info, time.Unix(infoItem.ExpiryTime, 0), nil
}

func toStoreItem(state *accountinfo.State) (map[string]dynamodb.AttributeValue, error) {
	if state.Account == nil || state.Owner == nil || state.Slot == 0 {
		return nil, errors.New("account, owner and slot must all be set")
	}

	return dynamodbattribute.MarshalMap(&storeItem{
		Account: state.Account,
		Owner:   state.Owner,
		Balance: state.Balance,
		Slot:    state.Slot,
	})
}

func fromStoreItem(item map[string]dynamodb.AttributeValue) (state *accountinfo.State, err error) {
	var storeItem storeItem
	if err := dynamodbattribute.UnmarshalMap(item, &storeItem); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal account info item")
	}

	return &accountinfo.State{
		Account: storeItem.Account,
		Owner:   storeItem.Owner,
		Balance: storeItem.Balance,
		Slot:    storeItem.Slot,
	}, nil
}
