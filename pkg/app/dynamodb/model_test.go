package dynamodb

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora-transaction-services-internal/pkg/app"
)

func TestValidModelConversion(t *testing.T) {
	config := &app.Config{
		AppName:            "kin",
		AgoraDataURL:       "test.kin.org/agoradata",
		SignTransactionURL: "test.kin.org/signtx",
	}

	item, err := toItem(0, config)
	require.NoError(t, err)
	require.Equal(t, aws.StringValue(item["app_index"].N), "0")
	require.Equal(t, aws.StringValue(item["app_name"].S), config.AppName)
	require.Equal(t, aws.StringValue(item["agora_data_url"].S), config.AgoraDataURL)
	require.Equal(t, aws.StringValue(item["sign_transaction_url"].S), config.SignTransactionURL)

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
