package dynamodb

import (
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	dynamotest "github.com/kinecosystem/agora-common/aws/dynamodb/test"
	"github.com/ory/dockertest"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion/tests"
)

var (
	testLockCtor tests.LockCtor
	teardown     func()
	dynamoClient dynamodbiface.DynamoDBAPI
)

func TestMain(m *testing.M) {
	log := logrus.StandardLogger()

	testPool, err := dockertest.NewPool("")
	if err != nil {
		log.WithError(err).Error("Error creating docker pool")
		os.Exit(1)
	}

	var cleanUpFunc func()
	dynamoClient, cleanUpFunc, err = dynamotest.StartDynamoDBV1(testPool)
	if err != nil {
		log.WithError(err).Error("Error starting dynamoDB image")
		os.Exit(1)
	}

	if err := setupTestTable(dynamoClient); err != nil {
		log.WithError(err).Error("Error creating test tables")
		cleanUpFunc()
		os.Exit(1)
	}

	testLockCtor = func(key string) (ingestion.DistributedLock, error) {
		return New(dynamoClient, key, time.Second)
	}
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
	tests.RunLockTests(t, testLockCtor, teardown)
}

func setupTestTable(client dynamodbiface.DynamoDBAPI) error {
	keySchema := []*dynamodb.KeySchemaElement{
		{
			AttributeName: aws.String("key"),
			KeyType:       aws.String(dynamodb.KeyTypeHash),
		},
	}
	attrDefinitions := []*dynamodb.AttributeDefinition{
		{
			AttributeName: aws.String("key"),
			AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
		},
	}
	_, err := client.CreateTable(&dynamodb.CreateTableInput{
		KeySchema:            keySchema,
		AttributeDefinitions: attrDefinitions,
		BillingMode:          aws.String(dynamodb.BillingModePayPerRequest),
		TableName:            aws.String(tableName),
	})
	return err
}

func resetTestTable(client dynamodbiface.DynamoDBAPI) error {
	for _, table := range []string{tableName} {
		_, err := client.DeleteTable(&dynamodb.DeleteTableInput{
			TableName: aws.String(table),
		})
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
