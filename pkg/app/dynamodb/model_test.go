package dynamodb

import (
	"crypto/ed25519"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/mr-tron/base58/base58"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/app"
)

func TestModelConversion_Full(t *testing.T) {
	createAccountURLStr := "test.kin.org/create"
	signTxURLStr := "test.kin.org/signtx"
	eventsURLStr := "test.kin.org/events"

	createAccountURL, err := url.Parse(createAccountURLStr)
	require.NoError(t, err)

	signTxURL, err := url.Parse(signTxURLStr)
	require.NoError(t, err)

	eventsURL, err := url.Parse(eventsURLStr)
	require.NoError(t, err)

	subsidizer, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	config := &app.Config{
		AppName:            "kin",
		CreateAccountURL:   createAccountURL,
		SignTransactionURL: signTxURL,
		EventsURL:          eventsURL,
		Subsidizer:         subsidizer,
	}

	item, err := toItem(1, config)
	require.NoError(t, err)
	require.Equal(t, aws.StringValue(item["app_index"].N), "1")
	require.Equal(t, aws.StringValue(item["app_name"].S), config.AppName)
	require.Equal(t, aws.StringValue(item["create_account_url"].S), createAccountURLStr)
	require.Equal(t, aws.StringValue(item["sign_transaction_url"].S), signTxURLStr)
	require.Equal(t, aws.StringValue(item["events_url"].S), eventsURLStr)
	require.Equal(t, aws.StringValue(item["subsidizer"].S), base58.Encode(subsidizer))

	convertedConfig, err := fromItem(item)
	require.NoError(t, err)
	require.Equal(t, convertedConfig, config)
}

func TestModelConversion_WithEmpty(t *testing.T) {
	config := &app.Config{
		AppName: "kin",
	}

	item, err := toItem(1, config)
	require.NoError(t, err)
	require.Equal(t, aws.StringValue(item["app_index"].N), "1")
	require.Equal(t, aws.StringValue(item["app_name"].S), config.AppName)

	_, ok := item["sign_transaction_url"]
	require.False(t, ok)

	_, ok = item["events_url"]
	require.False(t, ok)

	_, ok = item["create_account_url"]
	require.False(t, ok)

	_, ok = item["subsidizer"]
	require.False(t, ok)

	convertedConfig, err := fromItem(item)
	require.NoError(t, err)
	require.Equal(t, convertedConfig, config)
}

func TestInvalidConversionToConfigItem(t *testing.T) {
	_, err := toItem(0, &app.Config{AppName: "kin"})
	require.Error(t, err)

	_, err = toItem(1, nil)
	require.Error(t, err)

	_, err = toItem(1, &app.Config{})
	require.Error(t, err)

	_, err = fromItem(map[string]dynamodb.AttributeValue{
		"app_name":   {S: aws.String("kin")},
		"subsidizer": {S: aws.String("test")},
	})
	require.Error(t, err)
}
