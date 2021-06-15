package dynamodb

import (
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v4"
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v4"

	"github.com/kinecosystem/agora/pkg/testutil"
)

func TestCacheModelConversion(t *testing.T) {
	key := testutil.GenerateSolanaKeys(t, 1)[0]
	info := &accountpb.AccountInfo{
		AccountId: &commonpb.SolanaAccountId{Value: key},
	}
	expiry := time.Now().Add(time.Minute)

	item, err := toItem(info, expiry)
	require.NoError(t, err)

	assert.EqualValues(t, key, item["key"].B)
	b, err := proto.Marshal(info)
	require.NoError(t, err)
	assert.EqualValues(t, b, item["account_info"].B)

	i, err := strconv.Atoi(aws.StringValue(item["expiry_time"].N))
	require.NoError(t, err)
	assert.EqualValues(t, expiry.Unix(), i)

	converted, convertedExpiry, err := fromItem(item)
	require.NoError(t, err)
	assert.EqualValues(t, expiry.Unix(), convertedExpiry.Unix())

	assert.True(t, proto.Equal(info, converted))
}
