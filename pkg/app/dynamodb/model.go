package dynamodb

import (
	"net/url"

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
	AppIndex           uint16 `dynamodbav:"app_index"`
	AppName            string `dynamodbav:"app_name,omitempty"`
	AgoraDataURL       string `dynamodbav:"agora_data_url,omitempty"`
	SignTransactionURL string `dynamodbav:"sign_transaction_url,omitempty"`
	InvoicingEnabled   bool   `dynamodbav:"invoicing_enabled,omitempty"`
}

func toItem(appIndex uint16, config *app.Config) (map[string]dynamodb.AttributeValue, error) {
	if config == nil {
		return nil, errors.New("config is nil")
	}

	if len(config.AppName) == 0 {
		return nil, errors.New("app name has length of 0")
	}

	configItem := &configItem{
		AppIndex:         appIndex,
		AppName:          config.AppName,
		InvoicingEnabled: config.InvoicingEnabled,
	}

	if config.AgoraDataURL != nil {
		configItem.AgoraDataURL = config.AgoraDataURL.String()
	}

	if config.SignTransactionURL != nil {
		configItem.SignTransactionURL = config.SignTransactionURL.String()
	}

	return dynamodbattribute.MarshalMap(configItem)
}

func fromItem(item map[string]dynamodb.AttributeValue) (*app.Config, error) {
	var configItem configItem
	if err := dynamodbattribute.UnmarshalMap(item, &configItem); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal config")
	}

	config := &app.Config{
		AppName:          configItem.AppName,
		InvoicingEnabled: configItem.InvoicingEnabled,
	}

	if len(configItem.AgoraDataURL) != 0 {
		agoraDataURL, err := url.Parse(configItem.AgoraDataURL)
		if err != nil {
			return nil, errors.Wrapf(err, "error parsing agora data url")
		}
		config.AgoraDataURL = agoraDataURL
	}

	if len(configItem.SignTransactionURL) != 0 {
		signTxURL, err := url.Parse(configItem.SignTransactionURL)
		if err != nil {
			return nil, errors.Wrapf(err, "error parsing sign transaction url")
		}
		config.SignTransactionURL = signTxURL
	}

	return config, nil
}
