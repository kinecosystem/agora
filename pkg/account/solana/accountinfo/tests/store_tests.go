package tests

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/account/solana/accountinfo"
	"github.com/kinecosystem/agora/pkg/testutil"
)

type testCase func(t *testing.T, store accountinfo.StateStore, teardown func())

func RunStateStoreTests(t *testing.T, store accountinfo.StateStore, teardown func()) {
	for _, test := range []testCase{testStoreRoundTrip, testGetOwnersByAccount} {
		test(t, store, teardown)
	}
}

func testStoreRoundTrip(t *testing.T, store accountinfo.StateStore, teardown func()) {
	t.Run("testRoundTrip", func(t *testing.T) {
		defer teardown()

		accounts := testutil.GenerateSolanaKeys(t, 2)
		owners := testutil.GenerateSolanaKeys(t, 2)
		balances := []int64{10, 15}
		slot := uint64(10)

		// Verify doesn't exist
		state, err := store.Get(context.Background(), accounts[0])
		assert.Equal(t, accountinfo.ErrNotFound, err)
		assert.Nil(t, state)

		require.NoError(t, store.Put(context.Background(), &accountinfo.State{
			Account: accounts[0],
			Owner:   owners[0],
			Balance: balances[0],
			Slot:    slot,
		}))
		require.NoError(t, store.Put(context.Background(), &accountinfo.State{
			Account: accounts[1],
			Owner:   owners[1],
			Balance: balances[1],
			Slot:    slot,
		}))

		// Assert all added
		for i, k := range accounts {
			state, err := store.Get(context.Background(), k)
			require.NoError(t, err)
			assert.NotNil(t, state)
			assert.Equal(t, owners[i], state.Owner)
			assert.Equal(t, balances[i], state.Balance)
			assert.Equal(t, slot, state.Slot)
		}

		require.NoError(t, store.Delete(context.Background(), accounts[0]))
		state, err = store.Get(context.Background(), accounts[0])
		assert.Equal(t, accountinfo.ErrNotFound, err)
		assert.Nil(t, state)
	})
}

func testGetOwnersByAccount(t *testing.T, store accountinfo.StateStore, teardown func()) {
	t.Run("testGetOwnersByAccount", func(t *testing.T) {
		defer teardown()

		accounts := testutil.GenerateSolanaKeys(t, 3)
		sortKeys(accounts) // makes validation easier later
		owners := testutil.GenerateSolanaKeys(t, 2)

		// Make owners[0] own accounts[0] and accounts[1]
		// Make owners[1] own accounts[2]
		require.NoError(t, store.Put(context.Background(), &accountinfo.State{
			Account: accounts[0],
			Owner:   owners[0],
			Slot:    1,
		}))
		require.NoError(t, store.Put(context.Background(), &accountinfo.State{
			Account: accounts[1],
			Owner:   owners[0],
			Slot:    1,
		}))
		require.NoError(t, store.Put(context.Background(), &accountinfo.State{
			Account: accounts[2],
			Owner:   owners[1],
			Slot:    1,
		}))

		owner0Accounts, err := store.GetAccountsByOwner(context.Background(), owners[0])
		require.NoError(t, err)
		assert.Equal(t, 2, len(owner0Accounts))
		sortKeys(owner0Accounts)
		for i, a := range []ed25519.PublicKey{accounts[0], accounts[1]} {
			assert.EqualValues(t, a, owner0Accounts[i])
		}

		owner1Accounts, err := store.GetAccountsByOwner(context.Background(), owners[1])
		require.NoError(t, err)
		assert.Equal(t, 1, len(owner1Accounts))
		assert.EqualValues(t, accounts[2], owner1Accounts[0])

		// Test change in ownership
		require.NoError(t, store.Put(context.Background(), &accountinfo.State{
			Account: accounts[2],
			Owner:   owners[0],
			Slot:    1,
		}))

		owner0Accounts, err = store.GetAccountsByOwner(context.Background(), owners[0])
		require.NoError(t, err)
		assert.Equal(t, 3, len(owner0Accounts))
		sortKeys(owner0Accounts)
		for i, a := range accounts {
			assert.EqualValues(t, a, owner0Accounts[i])
		}

		owner1Accounts, err = store.GetAccountsByOwner(context.Background(), owners[1])
		require.NoError(t, err)
		assert.Equal(t, 0, len(owner1Accounts))
	})
}

func sortKeys(src []ed25519.PublicKey) {
	sort.Slice(src, func(i, j int) bool {
		return bytes.Compare(src[i], src[j]) < 0
	})
}
