package dynamodb

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"github.com/pkg/errors"

	"github.com/kinecosystem/agora-transaction-services-internal/pkg/app"
)

const (
	tableName    = "app-configs"
	putCondition = "attribute_not_exists(app_index)"

	tableHashKey = "app_index"
)

var (
	tableNameStr    = aws.String(tableName)
	putConditionStr = aws.String(putCondition)
)

type configItem struct {
	AppIndex           int    `dynamodbav:"app_index"`
	AppName            string `dynamodbav:"app_name"`
	AgoraDataURL       string `dynamodbav:"agora_data_url"`
	SignTransactionURL string `dynamodbav:"sign_transaction_url"`
}

func toItem(appIndex uint16, config *app.Config) (map[string]dynamodb.AttributeValue, error) {
	if config == nil {
		return nil, errors.New("config is nil")
	}

	if len(config.AppName) == 0 {
		return nil, errors.New("app name has length of 0")
	}

	return dynamodbattribute.MarshalMap(&configItem{
		AppIndex:           int(appIndex),
		AppName:            config.AppName,
		AgoraDataURL:       config.AgoraDataURL,
		SignTransactionURL: config.SignTransactionURL,
	})
}

func fromItem(item map[string]dynamodb.AttributeValue) (*app.Config, error) {
	var configItem configItem
	if err := dynamodbattribute.UnmarshalMap(item, &configItem); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal config")
	}

	return &app.Config{
		AppName:            configItem.AppName,
		AgoraDataURL:       configItem.AgoraDataURL,
		SignTransactionURL: configItem.SignTransactionURL,
	}, nil
}
