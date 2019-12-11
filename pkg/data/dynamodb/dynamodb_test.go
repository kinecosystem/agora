package dynamodb

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbiface"
	"github.com/ory/dockertest"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/kin-api/genproto/common/v3"

	dynamotest "github.com/kinecosystem/agora-common/aws/dynamodb/test"
	"github.com/kinecosystem/agora-common/env"
	"github.com/kinecosystem/agora-transaction-services/pkg/data"
	"github.com/kinecosystem/agora-transaction-services/pkg/data/tests"
)

var (
	testStore    data.Store
	teardown     func()
	dynamoClient dynamodbiface.ClientAPI

	tableNameStr = aws.String(fmt.Sprintf("%s-%s", baseTableName, env.AgoraEnvironmentTest))
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

	testStore, err = New(env.AgoraEnvironmentTest, dynamoClient)
	if err != nil {
		log.WithError(err).Error("Error creating store")
		cleanUpFunc()
		os.Exit(1)
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
	tests.RunTests(t, testStore, teardown)
}

func TestBadData(t *testing.T) {
	defer teardown()

	fk := make([]byte, 32)
	for i := byte(0); i < 32; i++ {
		fk[i] = i
	}

	d := &common.AgoraData{
		Title:           "Test",
		Description:     "abc",
		TransactionType: common.AgoraData_EARN,
		ForeignKey:      fk,
	}

	item, err := marshalData(d)
	require.NoError(t, err)

	item["data"] = dynamodb.AttributeValue{B: []byte{3, 2, 1}}

	_, err = dynamoClient.PutItemRequest(&dynamodb.PutItemInput{
		TableName: tableNameStr,
		Item:      item,
	}).Send(context.Background())
	require.NoError(t, err)

	// This should fail, but we just want to ensure we don't crash
	data, err := testStore.Get(context.Background(), fk)
	require.NotNil(t, err)
	require.Nil(t, data)
}

func setupTestTable(client dynamodbiface.ClientAPI) error {
	keySchema := []dynamodb.KeySchemaElement{
		{
			AttributeName: aws.String("prefix"),
			KeyType:       dynamodb.KeyTypeHash,
		},
		{
			AttributeName: aws.String("suffix"),
			KeyType:       dynamodb.KeyTypeRange,
		},
	}

	attrDefinitions := []dynamodb.AttributeDefinition{
		{
			AttributeName: aws.String("prefix"),
			AttributeType: dynamodb.ScalarAttributeTypeB,
		},
		{
			AttributeName: aws.String("suffix"),
			AttributeType: dynamodb.ScalarAttributeTypeB,
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
	_, err := client.DeleteTableRequest(&dynamodb.DeleteTableInput{
		TableName: tableNameStr,
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

	return setupTestTable(client)
}
