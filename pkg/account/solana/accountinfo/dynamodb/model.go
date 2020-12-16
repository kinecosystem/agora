package dynamodb

import (
	"crypto/ed25519"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v4"
)

const (
	tableName = "account-info"

	tableHashKey = "key"
	ttlKey       = "expiry_time"
)

var (
	tableNameStr = aws.String(tableName)
	ttlKeyStr    = aws.String(ttlKey)
)

type infoItem struct {
	Key         []byte `dynamodbav:"key"`
	AccountInfo []byte `dynamodbav:"account_info"`
	ExpiryTime  int64  `dynamodbav:"expiry_time"`
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
