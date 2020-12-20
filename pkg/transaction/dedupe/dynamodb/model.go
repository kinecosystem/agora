package dynamodb

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	transactionpb "github.com/kinecosystem/agora-api/genproto/transaction/v4"
	"github.com/kinecosystem/agora/pkg/transaction/dedupe"
)

type infoItem struct {
	ID             []byte `dynamodbav:"id"`
	Signature      []byte `dynamodbav:"sig"`
	Response       []byte `dynamodbav:"resp"`
	SubmissionTime int64  `dynamodbav:"stime"`
	TTL            int64  `dynamodbav:"ttl"`
}

func getItem(id []byte, info *dedupe.Info, ttl time.Duration) (map[string]dynamodb.AttributeValue, error) {
	ii := &infoItem{
		ID:             id,
		Signature:      info.Signature,
		SubmissionTime: info.SubmissionTime.Unix(),
		TTL:            info.SubmissionTime.Add(ttl).Unix(),
	}

	if info.Response != nil {
		var err error
		ii.Response, err = proto.Marshal(info.Response)
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal info response")
		}
	}

	return dynamodbattribute.MarshalMap(ii)
}

func getInfo(item map[string]dynamodb.AttributeValue) (*dedupe.Info, error) {
	var ii infoItem
	if err := dynamodbattribute.UnmarshalMap(item, &ii); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal item")
	}

	info := &dedupe.Info{
		Signature:      ii.Signature,
		SubmissionTime: time.Unix(ii.SubmissionTime, 0),
	}

	if len(ii.Response) > 0 {
		info.Response = &transactionpb.SubmitTransactionResponse{}
		if err := proto.Unmarshal(ii.Response, info.Response); err != nil {
			return nil, errors.Wrap(err, "invalid response data")
		}
	}

	return info, nil
}
