package dynamodb

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbiface"
	dynamotest "github.com/kinecosystem/agora-common/aws/dynamodb/test"
	"github.com/ory/dockertest"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion/tests"
)

var (
	testCommitter ingestion.Committer
	teardown      func()
	dynamoClient  dynamodbiface.ClientAPI
)

func TestMain(m *testing.M) {
	log := logrus.StandardLogger()

	testPool, err := dockertest.NewPool("")
	if err != nil {
		log.WithError(err).Error("Error creating docker pool")
		os.Exit(1)
	}

	var cleanUpFunc func()
	dynamoClient, cleanUpFunc, err = dynamotest.StartDynamoDB(testPool)
	if err != nil {
		log.WithError(err).Error("Error starting dynamoDB image")
		os.Exit(1)
	}

	if err := setupTestTable(dynamoClient); err != nil {
		log.WithError(err).Error("Error creating test tables")
		cleanUpFunc()
		os.Exit(1)
	}

	testCommitter = New(dynamoClient)
	teardown = func() {
		if pc := recover(); pc != nil {
			cleanUpFunc()
			panic(pc)
		}

		if err := resetTestTable(dynamoClient); err != nil {
			logrus.StandardLogger().WithError(err).Error("Error resetting test tables")
			cleanUpFunc()
			os.Exit(1)
		}
	}

	code := m.Run()
	cleanUpFunc()
	os.Exit(code)
}

func TestStore(t *testing.T) {
	tests.RunCommitterTests(t, testCommitter, teardown)
}

func TestCommitUnsafe(t *testing.T) {
	c := testCommitter.(*Committer)

	p1 := []byte{0, 1, 2}
	p2 := []byte{2, 1, 0}

	assert.NoError(t, c.CommitUnsafe(context.Background(), "test", p1))
	actual, err := c.Latest(context.Background(), "test")
	assert.NoError(t, err)
	assert.EqualValues(t, p1, actual)

	assert.NoError(t, c.CommitUnsafe(context.Background(), "test", p2))
	actual, err = c.Latest(context.Background(), "test")
	assert.NoError(t, err)
	assert.EqualValues(t, p2, actual)

	assert.NoError(t, c.CommitUnsafe(context.Background(), "test", p1))
	actual, err = c.Latest(context.Background(), "test")
	assert.NoError(t, err)
	assert.EqualValues(t, p1, actual)
}

func setupTestTable(client dynamodbiface.ClientAPI) error {
	keySchema := []dynamodb.KeySchemaElement{
		{
			AttributeName: aws.String(ingestorKey),
			KeyType:       dynamodb.KeyTypeHash,
		},
	}
	attrDefinitions := []dynamodb.AttributeDefinition{
		{
			AttributeName: aws.String(ingestorKey),
			AttributeType: dynamodb.ScalarAttributeTypeS,
		},
	}
	_, err := client.CreateTableRequest(&dynamodb.CreateTableInput{
		KeySchema:            keySchema,
		AttributeDefinitions: attrDefinitions,
		BillingMode:          dynamodb.BillingModePayPerRequest,
		TableName:            tableNameStr,
	}).Send(context.Background())
	return err
}

func resetTestTable(client dynamodbiface.ClientAPI) error {
	for _, table := range []string{tableName} {
		_, err := client.DeleteTableRequest(&dynamodb.DeleteTableInput{
			TableName: aws.String(table),
		}).Send(context.Background())
		if err != nil {
			if awsErr, ok := err.(awserr.Error); ok {
				if awsErr.Code() != dynamodb.ErrCodeResourceNotFoundException {
					return errors.Wrap(err, "failed to delete table")
				}
			} else {
				return errors.Wrap(err, "failed to delete table")
			}
		}
	}

	return setupTestTable(client)
}
