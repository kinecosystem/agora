package dynamodb

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	"github.com/aws/aws-sdk-go-v2/aws/ec2metadata"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbiface"
	kinkeypair "github.com/kinecosystem/go/keypair"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/kinecosystem/agora/pkg/keypair"
)

const StoreType = "dynamodb"

const (
	tableKey         = "id"
	tableEnvVarKey   = "KEYSTORE_DYNAMO_TABLE"
	defaultTableName = "keypairs"
)

var (
	putKeypairConditionStr = aws.String("attribute_not_exists(id)")
)

type store struct {
	log       *logrus.Entry
	client    dynamodbiface.ClientAPI
	tableName *string
}

func init() {
	keypair.RegisterStoreConstructor(StoreType, newStore)
}

// newStore returns keypair.Keystore backed by DynamoDB
func newStore() (keypair.Keystore, error) {
	log := logrus.StandardLogger().WithField("type", "keypair/dynamodb")

	awsConfig, err := external.LoadDefaultAWSConfig()
	if err != nil {
		return nil, errors.Wrap(err, "failed to load default aws config")
	}

	if awsConfig.Region == "" {
		region, err := ec2metadata.New(awsConfig).Region()
		if err != nil {
			return nil, errors.Wrap(err, "failed to get region from ec2 metadata")
		}
		awsConfig.Region = region
	}

	dynamoDBClient := dynamodb.New(awsConfig)
	tableName := os.Getenv(tableEnvVarKey)
	if len(tableName) == 0 {
		log.Infof("table name not configured, using default (%s)", defaultTableName)
		tableName = defaultTableName
	}

	return &store{
		log:       log,
		client:    dynamoDBClient,
		tableName: aws.String(tableName),
	}, nil
}

// Put implements Keystore.Put
func (s *store) Put(ctx context.Context, id string, full *kinkeypair.Full) error {
	item, err := toKeypairItem(id, full)
	if err != nil {
		return errors.Wrapf(err, "failed to convert full keypair")
	}

	req := &dynamodb.PutItemInput{
		TableName:           s.tableName,
		Item:                item,
		ConditionExpression: putKeypairConditionStr,
	}
	if _, err = s.client.PutItemRequest(req).Send(ctx); err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				return keypair.ErrKeypairAlreadyExists
			}
		}
		return errors.Wrapf(err, "failed to put keypair")
	}

	return nil
}

// Get implements Keystore.Get
func (s *store) Get(ctx context.Context, id string) (*kinkeypair.Full, error) {
	resp, err := s.client.GetItemRequest(&dynamodb.GetItemInput{
		TableName: s.tableName,
		Key: map[string]dynamodb.AttributeValue{
			tableKey: {S: aws.String(id)},
		},
	}).Send(ctx)

	if err != nil {
		return nil, errors.Wrapf(err, "failed to get keypair")
	}

	if len(resp.Item) == 0 {
		return nil, keypair.ErrKeypairNotFound
	}

	full, err := fromKeypairItem(resp.Item)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal keypair")
	}

	return full, nil
}
