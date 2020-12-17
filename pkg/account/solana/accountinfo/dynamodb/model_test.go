package dynamodb

import (
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/agora/pkg/account/solana/accountinfo"
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

func TestStoreModelConversion(t *testing.T) {
	state := &accountinfo.State{
		Account: testutil.GenerateSolanaKeys(t, 1)[0],
		Owner:   testutil.GenerateSolanaKeys(t, 1)[0],
		Balance: 10,
		Slot:    20,
	}
	item, err := toStoreItem(state)
	require.NoError(t, err)
	require.NotNil(t, item)

	assert.EqualValues(t, state.Account, item["account"].B)
	assert.EqualValues(t, state.Owner, item["owner"].B)
	assert.Equal(t, "10", *item["balance"].N)
	assert.Equal(t, "20", *item["slot"].N)

	stored, err := fromStoreItem(item)
	require.NoError(t, err)
	require.NotNil(t, stored)

	assert.EqualValues(t, *state, *stored)
}
