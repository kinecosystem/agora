package dynamodb

import (
	"crypto/ed25519"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"github.com/mr-tron/base58/base58"
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
	CreateAccountURL   string `dynamodbav:"create_account_url,omitempty"`
	SignTransactionURL string `dynamodbav:"sign_transaction_url,omitempty"`
	EventsURL          string `dynamodbav:"events_url,omitempty"`
	WebhookSecret      string `dynamodbav:"webhook_secret,omitempty"`
	Subsidizer         string `dynamodbav:"subsidizer,omitempty"`
}

func toItem(appIndex uint16, config *app.Config) (map[string]dynamodb.AttributeValue, error) {
	if appIndex == 0 {
		return nil, errors.New("cannot add config for app index 0")
	}

	if config == nil {
		return nil, errors.New("config is nil")
	}

	if len(config.AppName) == 0 {
		return nil, errors.New("app name has length of 0")
	}

	configItem := &configItem{
		AppIndex:      appIndex,
		AppName:       config.AppName,
		WebhookSecret: config.WebhookSecret,
	}

	if config.CreateAccountURL != nil {
		configItem.CreateAccountURL = config.CreateAccountURL.String()
	}
	if config.SignTransactionURL != nil {
		configItem.SignTransactionURL = config.SignTransactionURL.String()
	}
	if config.EventsURL != nil {
		configItem.EventsURL = config.EventsURL.String()
	}
	if len(config.Subsidizer) == ed25519.PublicKeySize {
		configItem.Subsidizer = base58.Encode(config.Subsidizer)
	}

	return dynamodbattribute.MarshalMap(configItem)
}

func fromItem(item map[string]dynamodb.AttributeValue) (*app.Config, error) {
	var configItem configItem
	if err := dynamodbattribute.UnmarshalMap(item, &configItem); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal config")
	}

	config := &app.Config{
		AppName:       configItem.AppName,
		WebhookSecret: configItem.WebhookSecret,
	}

	if len(configItem.CreateAccountURL) != 0 {
		createAccountURL, err := url.Parse(configItem.CreateAccountURL)
		if err != nil {
			return nil, errors.Wrapf(err, "error parsing create account url")
		}
		config.CreateAccountURL = createAccountURL
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
	if len(configItem.Subsidizer) != 0 {
		raw, err := base58.Decode(configItem.Subsidizer)
		if err != nil {
			return nil, errors.Wrap(err, "error parsing subsidizer")
		}
		if len(raw) != ed25519.PublicKeySize {
			return nil, errors.Errorf("invalid subsidizer key size: %d", len(raw))
		}

		config.Subsidizer = raw
	}

	return config, nil
}
