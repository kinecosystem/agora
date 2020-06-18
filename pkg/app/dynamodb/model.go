package dynamodb

import (
	"net/url"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"github.com/pkg/errors"

	"github.com/kinecosystem/agora/pkg/app"
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
	SignTransactionURL string `dynamodbav:"sign_transaction_url,omitempty"`
	EventsURL          string `dynamodbav:"events_url,omitempty"`
	InvoicingEnabled   bool   `dynamodbav:"invoicing_enabled,omitempty"`
	WebhookSecret      []byte `dynamodbav:"webhook_secret,omitempty"`
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
		WebhookSecret:    config.WebhookSecret,
	}

	if config.SignTransactionURL != nil {
		configItem.SignTransactionURL = config.SignTransactionURL.String()
	}
	if config.EventsURL != nil {
		configItem.EventsURL = config.EventsURL.String()
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
		WebhookSecret:    configItem.WebhookSecret,
	}

	if len(configItem.SignTransactionURL) != 0 {
		signTxURL, err := url.Parse(configItem.SignTransactionURL)
		if err != nil {
			return nil, errors.Wrapf(err, "error parsing sign transaction url")
		}
		config.SignTransactionURL = signTxURL
	}
	if len(configItem.EventsURL) != 0 {
		eventsURL, err := url.Parse(configItem.EventsURL)
		if err != nil {
			return nil, errors.Wrapf(err, "error parsing sign transaction url")
		}
		config.EventsURL = eventsURL
	}

	return config, nil
}
