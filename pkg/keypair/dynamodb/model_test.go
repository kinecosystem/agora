package dynamodb

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/kinecosystem/go/keypair"
	"github.com/stretchr/testify/require"
)

func TestValidModelConversion(t *testing.T) {
	kp, err := keypair.Random()
	require.NoError(t, err)

	item, err := toKeypairItem(kp.Address(), kp)
	require.NoError(t, err)
	require.Equal(t, kp.Address(), *item["id"].S)
	require.Equal(t, kp.Seed(), *item["seed"].S)

	convertedKP, err := fromKeypairItem(item)
	require.NoError(t, err)
	require.Equal(t, kp, convertedKP)
}

func TestInvalidModelConversionToKeypairItem(t *testing.T) {
	_, err := toKeypairItem("", nil)
	require.Error(t, err)

	_, err = toKeypairItem("someid", nil)
	require.Error(t, err)
}

func TestInvalidConverstionFromKeypairItem(t *testing.T) {
	itemValue := make(map[string]dynamodb.AttributeValue)

	_, err := fromKeypairItem(itemValue)
	require.Error(t, err)
}
