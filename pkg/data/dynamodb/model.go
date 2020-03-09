package dynamodb

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	commonpb "github.com/kinecosystem/kin-api/genproto/common/v3"
)

const (
	tableName    = "transaction-data"
	putCondition = "attribute_not_exists(prefix)"
)

var (
	tableNameStr    = aws.String(tableName)
	putConditionStr = aws.String(putCondition)
)

type marshalledEntry struct {
	Prefix []byte `dynamodbav:"prefix"`
	Suffix []byte `dynamodbav:"suffix"`
	Data   []byte `dynamodbav:"data"`
}

func toItem(data *commonpb.AgoraData) (map[string]dynamodb.AttributeValue, error) {
	if len(data.ForeignKey) != 32 {
		return nil, errors.New("fk must be 32 bytes")
	}

	b, err := proto.Marshal(data)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal data")
	}

	return dynamodbattribute.MarshalMap(&marshalledEntry{
		Prefix: data.ForeignKey[:29],
		Suffix: data.ForeignKey[29:],
		Data:   b,
	})
}

func fromItem(item map[string]dynamodb.AttributeValue) (*commonpb.AgoraData, error) {
	var entry marshalledEntry
	if err := dynamodbattribute.UnmarshalMap(item, &entry); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal entry")
	}

	data := &commonpb.AgoraData{}
	if err := proto.Unmarshal(entry.Data, data); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal body")
	}

	return data, nil
}
