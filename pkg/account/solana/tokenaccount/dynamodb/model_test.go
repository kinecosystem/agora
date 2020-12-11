package dynamodb

import (
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/testutil"
)

func TestModelConversion(t *testing.T) {
	keys := testutil.GenerateSolanaKeys(t, 3)
	expiry := time.Now().Add(time.Minute)

	item, err := toItem(keys[0], keys[1:], expiry)
	require.NoError(t, err)

	assert.EqualValues(t, keys[0], item["owner"].B)
	assert.Len(t, item["token_accounts"].BS, len(keys[1:]))
	for i, key := range keys[1:] {
		assert.EqualValues(t, key, item["token_accounts"].BS[i])
	}

	i, err := strconv.Atoi(aws.StringValue(item["expiry_time"].N))
	require.NoError(t, err)
	assert.EqualValues(t, expiry.Unix(), i)

	owner, tokenAccounts, err := fromItem(item)
	require.NoError(t, err)

	assert.EqualValues(t, keys[0], owner)
	assert.EqualValues(t, keys[1:], tokenAccounts)
}
