package tests

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/account/solana/tokenaccount"
	"github.com/kinecosystem/agora/pkg/testutil"
)

type testCase func(t *testing.T, cache tokenaccount.Cache, teardown func())

func RunTests(t *testing.T, cache tokenaccount.Cache, teardown func()) {
	for _, test := range []testCase{testRoundTrip} {
		test(t, cache, teardown)
	}
}

func testRoundTrip(t *testing.T, cache tokenaccount.Cache, teardown func()) {
	t.Run("testRoundTrip", func(t *testing.T) {
		defer teardown()

		owners := testutil.GenerateSolanaKeys(t, 2)
		tokenAccounts := testutil.GenerateSolanaKeys(t, 2)

		// Verify doesn't exist
		cached, err := cache.Get(context.Background(), owners[0])
		assert.Equal(t, tokenaccount.ErrTokenAccountsNotFound, err)
		assert.Nil(t, cached)

		require.NoError(t, cache.Put(context.Background(), owners[0], tokenAccounts))
		require.NoError(t, cache.Put(context.Background(), owners[1], tokenAccounts))

		// Assert all added
		for _, owner := range owners {
			accounts, err := cache.Get(context.Background(), owner)
			require.NoError(t, err)
			assertTokenAccounts(t, tokenAccounts, accounts)
		}

		// Test Remove
		require.NoError(t, cache.Delete(context.Background(), owners[0]))

		cached, err = cache.Get(context.Background(), owners[0])
		assert.Equal(t, tokenaccount.ErrTokenAccountsNotFound, err)
		assert.Nil(t, cached)
	})
}

func assertTokenAccounts(t *testing.T, cached []ed25519.PublicKey, expected []ed25519.PublicKey) {
	assert.Len(t, cached, len(expected))

	sortKeys(cached)
	sortKeys(expected)
	for i, cachedAcc := range cached {
		assert.EqualValues(t, cachedAcc, expected[i])
	}
}

func sortKeys(src []ed25519.PublicKey) {
	sort.Slice(src, func(i, j int) bool { return bytes.Compare(src[i], src[j]) < 0 })
}
