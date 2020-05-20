package dynamodb

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"github.com/kinecosystem/go/keypair"
	"github.com/pkg/errors"
)

type keypairItem struct {
	ID   string `dynamodbav:"id"`
	Seed string `dynamodbav:"seed"`
}

func toKeypairItem(id string, full *keypair.Full) (map[string]dynamodb.AttributeValue, error) {
	if len(id) == 0 {
		return nil, errors.New("id cannot be empty")
	}
	if full == nil {
		return nil, errors.New("full keypair is nil")
	}

	return dynamodbattribute.MarshalMap(&keypairItem{
		ID:   id,
		Seed: full.Seed(),
	})
}

func fromKeypairItem(m map[string]dynamodb.AttributeValue) (*keypair.Full, error) {
	item := &keypairItem{}
	if err := dynamodbattribute.UnmarshalMap(m, item); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal keypair")
	}

	kp, err := keypair.Parse(item.Seed)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse seed")
	}

	full, ok := kp.(*keypair.Full)
	if !ok {
		return nil, errors.New("seed was invalid")
	}

	return full, nil
}
