package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v4"
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v4"

	"github.com/kinecosystem/agora/pkg/account/solana/accountinfo"
	"github.com/kinecosystem/agora/pkg/testutil"
)

const (
	TestTTL         = 2 * time.Second
	TestNegativeTTL = time.Second
)

type testCase func(t *testing.T, cache accountinfo.Cache, teardown func())

func RunTests(t *testing.T, cache accountinfo.Cache, teardown func()) {
	for _, test := range []testCase{testRoundTrip, testExpiry} {
		test(t, cache, teardown)
	}
}

func testRoundTrip(t *testing.T, cache accountinfo.Cache, teardown func()) {
	t.Run("testRoundTrip", func(t *testing.T) {
		defer teardown()

		keys := testutil.GenerateSolanaKeys(t, 2)
		infos := []*accountpb.AccountInfo{
			{
				AccountId: &commonpb.SolanaAccountId{Value: keys[0]},
				Balance:   10,
			},
			{
				AccountId: &commonpb.SolanaAccountId{Value: keys[1]},
				Balance:   10,
			},
		}

		// Verify doesn't exist
		cached, err := cache.Get(context.Background(), keys[0])
		assert.Equal(t, accountinfo.ErrAccountInfoNotFound, err)
		assert.Nil(t, cached)

		require.NoError(t, cache.Put(context.Background(), infos[0]))
		require.NoError(t, cache.Put(context.Background(), infos[1]))

		// Assert all added
		for i, k := range keys {
			info, err := cache.Get(context.Background(), k)
			require.NoError(t, err)
			assert.True(t, proto.Equal(infos[i], info))
		}

		// Delete works
		deleted, err := cache.Del(context.Background(), keys[0])
		require.NoError(t, err)
		assert.True(t, deleted)
		_, err = cache.Get(context.Background(), keys[0])
		assert.Equal(t, accountinfo.ErrAccountInfoNotFound, err)

		// Delete "deleted" is updated.
		deleted, err = cache.Del(context.Background(), keys[0])
		require.NoError(t, err)
		assert.False(t, deleted)

		// Other items unaffected
		info, err := cache.Get(context.Background(), keys[1])
		require.NoError(t, err)
		assert.True(t, proto.Equal(infos[1], info))
	})
}

func testExpiry(t *testing.T, cache accountinfo.Cache, teardown func()) {
	t.Run("testExpiry", func(t *testing.T) {
		defer teardown()

		key := testutil.GenerateSolanaKeys(t, 1)[0]
		info := &accountpb.AccountInfo{
			AccountId: &commonpb.SolanaAccountId{Value: key},
		}

		// Test expiry
		require.NoError(t, cache.Put(context.Background(), info))

		cached, err := cache.Get(context.Background(), key)
		require.NoError(t, err)
		assert.True(t, proto.Equal(info, cached))

		time.Sleep(TestTTL + 100*time.Millisecond)

		cached, err = cache.Get(context.Background(), key)
		assert.Equal(t, accountinfo.ErrAccountInfoNotFound, err)
		assert.Nil(t, cached)

		// Test negative expiry
		info.Balance = -1
		fmt.Println("ver")
		require.NoError(t, cache.Put(context.Background(), info))

		cached, err = cache.Get(context.Background(), key)
		require.NoError(t, err)
		assert.True(t, proto.Equal(info, cached))

		time.Sleep(TestNegativeTTL + 100*time.Millisecond)

		cached, err = cache.Get(context.Background(), key)
		assert.Equal(t, accountinfo.ErrAccountInfoNotFound, err)
		assert.Nil(t, cached)
	})
}
