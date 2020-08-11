package dynamodb

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"
)

func TestModelConversion_RoundTrip(t *testing.T) {
	item, err := toItem("test", 1)
	require.NoError(t, err)
	require.Equal(t, aws.StringValue(item["app_id"].S), "test")
	require.Equal(t, aws.StringValue(item["app_index"].N), "1")

	appIndex, err := fromItem(item)
	require.NoError(t, err)
	require.Equal(t, uint16(1), appIndex)
}

func TestModelConversion_Invalid(t *testing.T) {
	_, err := toItem("!!!", 1)
	require.Error(t, err)

	_, err = toItem("test", 0)
	require.Error(t, err)
}
