package dynamodb

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbiface"
	"github.com/ory/dockertest"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/kinecosystem/agora-common/aws/dynamodb/test"

	"github.com/kinecosystem/agora/pkg/keypair"
	"github.com/kinecosystem/agora/pkg/keypair/tests"
)

var (
	tableNameString = aws.String("test-keypairs")
	testStore       keypair.Keystore
	teardown        func()
)

func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		logrus.StandardLogger().WithError(err).Error("Error creating docker pool")
		os.Exit(1)
	}

	dynamoClient, cleanUpFunc, err := test.StartDynamoDB(pool)
	if err != nil {
		logrus.StandardLogger().WithError(err).Error("Error starting dynamoDB image")
		os.Exit(1)
	}

	if err := setupTestTable(dynamoClient); err != nil {
		logrus.StandardLogger().WithError(err).Error("Error creating keypair test table")
		cleanUpFunc()
		os.Exit(1)
	}

	testStore = &store{
		log:       logrus.StandardLogger().WithField("type", "keypair/dynamodb"),
		client:    dynamoClient,
		tableName: tableNameString,
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

func TestDynamoKeystore(t *testing.T) {
	tests.RunKeystoreTests(t, testStore, teardown)
}

func setupTestTable(db dynamodbiface.ClientAPI) error {
	_, err := db.DescribeTableRequest(&dynamodb.DescribeTableInput{
		TableName: tableNameString,
	}).Send(context.Background())
	if err != nil {
		_, err := createTestTable(db)
		if err != nil {
			return errors.Wrapf(err, "could not create test keypair table")
		}
	}
	return nil
}

func createTestTable(db dynamodbiface.ClientAPI) (*dynamodb.CreateTableResponse, error) {
	keySchema := []dynamodb.KeySchemaElement{
		{
			AttributeName: aws.String(tableKey),
			KeyType:       dynamodb.KeyTypeHash,
		},
	}

	attrDefinitions := []dynamodb.AttributeDefinition{
		{
			AttributeName: aws.String(tableKey),
			AttributeType: dynamodb.ScalarAttributeTypeS,
		},
	}

	return db.CreateTableRequest(&dynamodb.CreateTableInput{
		AttributeDefinitions: attrDefinitions,
		BillingMode:          "PAY_PER_REQUEST",
		KeySchema:            keySchema,
		TableName:            tableNameString,
	}).Send(context.Background())
}

func resetTestTable(db dynamodbiface.ClientAPI) error {
	_, err := db.DeleteTableRequest(&dynamodb.DeleteTableInput{
		TableName: tableNameString,
	}).Send(context.Background())
	if err != nil {
		_, err := createTestTable(db)
		if err != nil {
			return errors.Wrapf(err, "could not delete test keypair table")
		}
	}
	return setupTestTable(db)
}
