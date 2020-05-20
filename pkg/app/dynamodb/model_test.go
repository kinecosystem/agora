package dynamodb

import (
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/app"
)

func TestModelConversion_Full(t *testing.T) {
	signTxURLStr := "test.kin.org/signtx"

	signTxURL, err := url.Parse(signTxURLStr)
	require.NoError(t, err)

	config := &app.Config{
		AppName:            "kin",
		SignTransactionURL: signTxURL,
	}

	item, err := toItem(0, config)
	require.NoError(t, err)
	require.Equal(t, aws.StringValue(item["app_index"].N), "0")
	require.Equal(t, aws.StringValue(item["app_name"].S), config.AppName)
	require.Equal(t, aws.StringValue(item["sign_transaction_url"].S), signTxURLStr)

	convertedConfig, err := fromItem(item)
	require.NoError(t, err)
	require.Equal(t, convertedConfig, config)
}

func TestModelConversion_WithEmpty(t *testing.T) {
	config := &app.Config{
		AppName: "kin",
	}

	item, err := toItem(0, config)
	require.NoError(t, err)
	require.Equal(t, aws.StringValue(item["app_index"].N), "0")
	require.Equal(t, aws.StringValue(item["app_name"].S), config.AppName)

	_, ok := item["sign_transaction_url"]
	require.False(t, ok)

	convertedConfig, err := fromItem(item)
	require.NoError(t, err)
	require.Equal(t, convertedConfig, config)
}

func TestInvalidConversionToConfigItem(t *testing.T) {
	_, err := toItem(0, nil)
	require.Error(t, err)

	_, err = toItem(0, &app.Config{})
	require.Error(t, err)
}
