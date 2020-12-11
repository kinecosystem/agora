package memory

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/account/solana/tokenaccount"
	"github.com/kinecosystem/agora/pkg/account/solana/tokenaccount/tests"
	"github.com/kinecosystem/agora/pkg/testutil"
)

const (
	testTTL = time.Second
)

func TestCache(t *testing.T) {
	testCache, err := New(testTTL, 5)
	require.NoError(t, err)

	teardown := func() {
		testCache.(*cache).reset()
	}
	tests.RunTests(t, testCache, teardown)
}

func TestExpiry(t *testing.T) {
	testCache, err := New(testTTL, 5)
	require.NoError(t, err)

	keys := testutil.GenerateSolanaKeys(t, 2)

	// Test expiry
	require.NoError(t, testCache.Put(context.Background(), keys[0], keys[1:]))

	cached, err := testCache.Get(context.Background(), keys[0])
	require.NoError(t, err)
	assert.Len(t, cached, 1)
	assert.EqualValues(t, keys[1], cached[0])

	time.Sleep(testTTL)

	cached, err = testCache.Get(context.Background(), keys[1])
	assert.Equal(t, tokenaccount.ErrTokenAccountsNotFound, err)
	assert.Nil(t, cached)
}
