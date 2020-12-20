package dynamodb

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/aws"
	dynamotest "github.com/kinecosystem/agora-common/aws/dynamodb/test"
	"github.com/ory/dockertest"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/kinecosystem/agora/pkg/transaction/dedupe"
	"github.com/kinecosystem/agora/pkg/transaction/dedupe/tests"
)

var (
	testStore    dedupe.Deduper
	teardown     func()
	dynamoClient dynamodbiface.ClientAPI
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

	if err := setupTestTables(dynamoClient); err != nil {
		log.WithError(err).Error("Error creating test table")
		cleanUpFunc()
		os.Exit(1)
	}

	testStore = New(dynamoClient, time.Second)
	teardown = func() {
		if pc := recover(); pc != nil {
			cleanUpFunc()
			panic(pc)
		}

		if err := resetTestTables(dynamoClient); err != nil {
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
	tests.RunTests(t, testStore, teardown)
}

func setupTestTables(client dynamodbiface.ClientAPI) error {
	keySchema := []dynamodb.KeySchemaElement{
		{
			AttributeName: aws.String(idAttr),
			KeyType:       dynamodb.KeyTypeHash,
		},
	}

	attrDefinitions := []dynamodb.AttributeDefinition{
		{
			AttributeName: aws.String(idAttr),
			AttributeType: dynamodb.ScalarAttributeTypeB,
		},
	}

	_, err := client.CreateTableRequest(&dynamodb.CreateTableInput{
		KeySchema:            keySchema,
		AttributeDefinitions: attrDefinitions,
		BillingMode:          dynamodb.BillingModePayPerRequest,
		TableName:            tableStr,
	}).Send(context.Background())
	return err
}

func resetTestTables(client dynamodbiface.ClientAPI) error {
	_, err := client.DeleteTableRequest(&dynamodb.DeleteTableInput{
		TableName: tableStr,
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
	return setupTestTables(client)
}
