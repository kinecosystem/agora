package dynamodb

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/kinecosystem/kin-api/genproto/common/v3"
)

type marshalledEntry struct {
	Prefix []byte `dynamodbav:"prefix"`
	Suffix []byte `dynamodbav:"suffix"`
	Data   []byte `dynamodbav:"data"`
}

func marshalData(data *common.AgoraData) (map[string]dynamodb.AttributeValue, error) {
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

func unmarshalData(m map[string]dynamodb.AttributeValue) (*common.AgoraData, error) {
	var entry marshalledEntry
	if err := dynamodbattribute.UnmarshalMap(m, &entry); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal entry")
	}

	data := &common.AgoraData{}
	if err := proto.Unmarshal(entry.Data, data); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal body")
	}

	return data, nil
}
