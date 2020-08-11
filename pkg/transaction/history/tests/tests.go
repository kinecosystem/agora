package tests

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/testutil"
	"github.com/kinecosystem/agora/pkg/transaction/history"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
	historytestutil "github.com/kinecosystem/agora/pkg/transaction/history/model/testutil"
)

func RunTests(t *testing.T, rw history.ReaderWriter, teardown func()) {
	for _, tf := range []func(t *testing.T, rw history.ReaderWriter){
		testRoundTrip_Stellar,
		testOrdering,
		testDoubleInsert,
	} {
		tf(t, rw)
		teardown()
	}
}

func testRoundTrip_Stellar(t *testing.T, rw history.ReaderWriter) {
	t.Run("TestRoundTrip_Stellar", func(t *testing.T) {
		ctx := context.Background()
		accounts := testutil.GenerateAccountIDs(t, 10)
		entry, hash := historytestutil.GenerateEntry(t, 1, 1, accounts[0], accounts[1:], nil, nil)

		// Assert no previous state
		_, err := rw.GetTransaction(ctx, hash)
		assert.Equal(t, history.ErrNotFound, err)
		for i := 1; i < len(accounts); i++ {
			entries, err := rw.GetAccountTransactions(ctx, accounts[i].Address(), nil)
			assert.NoError(t, err)
			assert.Empty(t, entries)
		}

		require.NoError(t, rw.Write(ctx, entry))

		actual, err := rw.GetTransaction(ctx, hash)
		require.NoError(t, err)
		assert.True(t, proto.Equal(actual, entry))

		for i := 1; i < len(accounts); i++ {
			entries, err := rw.GetAccountTransactions(ctx, accounts[i].Address(), nil)
			assert.NoError(t, err)
			require.Len(t, entries, 1)
			require.True(t, proto.Equal(actual, entries[0]))
		}
	})
}

func testDoubleInsert(t *testing.T, rw history.ReaderWriter) {
	t.Run("TestDoubleInsert", func(t *testing.T) {
		ctx := context.Background()
		accounts := testutil.GenerateAccountIDs(t, 10)
		entry, hash := historytestutil.GenerateEntry(t, 1, 1, accounts[0], accounts[1:], nil, nil)

		// Writes should be idempotent, provided the entry is the same.
		for i := 0; i < 2; i++ {
			require.NoError(t, rw.Write(ctx, entry))

			actual, err := rw.GetTransaction(ctx, hash)
			require.NoError(t, err)
			assert.True(t, proto.Equal(actual, entry))

			entries, err := rw.GetAccountTransactions(ctx, accounts[0].Address(), nil)
			assert.NoError(t, err)
			require.Len(t, entries, 1)
			assert.True(t, proto.Equal(entry, entries[0]))
		}

		// Mutate the entry, then insert under the same "key"
		mutated := proto.Clone(entry).(*model.Entry)
		mutated.Kind.(*model.Entry_Stellar).Stellar.Ledger = 100
		assert.False(t, proto.Equal(entry, mutated))

		// If the entry doesn't match what was previously stored,
		// then there's some critical bug, _or_ the assumption that
		// entries are deterministically generated from transactions
		// is incorrect. Therefore, we expect an error from the writer
		// for safety.
		require.NotNil(t, rw.Write(ctx, mutated))

		// Ensure that the write did not go through

		actual, err := rw.GetTransaction(ctx, hash)
		require.NoError(t, err)
		assert.True(t, proto.Equal(actual, entry))
		assert.False(t, proto.Equal(actual, mutated))

		entries, err := rw.GetAccountTransactions(ctx, accounts[0].Address(), nil)
		assert.NoError(t, err)
		require.Len(t, entries, 1)
		assert.True(t, proto.Equal(entry, entries[0]))
		assert.False(t, proto.Equal(mutated, entries[0]))
	})
}

func testOrdering(t *testing.T, rw history.ReaderWriter) {
	t.Run("TestQuery", func(t *testing.T) {
		ctx := context.Background()
		accounts := testutil.GenerateAccountIDs(t, 10)

		_, err := rw.GetLatestForAccount(ctx, accounts[0].Address())
		assert.Equal(t, err, history.ErrNotFound)

		generated := make([]*model.Entry, 100)
		for i := 0; i < 100; i++ {
			generated[i], _ = historytestutil.GenerateEntry(t, uint64(i-i%2), i, accounts[0], accounts[1:], nil, nil)
			require.NoError(t, rw.Write(ctx, generated[i]))
		}

		latest, err := rw.GetLatestForAccount(ctx, accounts[0].Address())
		assert.NoError(t, err)
		assert.True(t, proto.Equal(generated[99], latest))

		lastKey, err := generated[99].GetOrderingKey()
		require.NoError(t, err)
		orderingKey, err := generated[10].GetOrderingKey()
		require.NoError(t, err)

		// Ascending, default limit
		for i := 1; i < len(accounts); i++ {
			entries, err := rw.GetAccountTransactions(ctx, accounts[i].Address(), nil)
			assert.NoError(t, err)
			require.Len(t, entries, 100)

			for e := 0; e < 100; e++ {
				require.True(t, proto.Equal(generated[e], entries[e]))
			}

			// Ascending, limit
			entries, err = rw.GetAccountTransactions(ctx, accounts[i].Address(), &history.ReadOptions{Limit: 50})
			assert.NoError(t, err)
			require.Len(t, entries, 50, "len: %v", len(entries))
			for e := 0; e < 50; e++ {
				require.True(t, proto.Equal(generated[e], entries[e]))
			}

			// Ascending, offset
			entries, err = rw.GetAccountTransactions(ctx, accounts[i].Address(), &history.ReadOptions{Start: orderingKey})
			assert.NoError(t, err)
			require.Len(t, entries, 90)
			for e := 0; e < 90; e++ {
				require.True(t, proto.Equal(generated[10+e], entries[e]))
			}

			// Descending, default limit
			//
			// We expect no results, since going "backwards" from 0 should return nothing
			entries, err = rw.GetAccountTransactions(ctx, accounts[i].Address(), &history.ReadOptions{Descending: true})
			assert.NoError(t, err)
			require.Len(t, entries, 0)

			// Descending, default limit + offset
			entries, err = rw.GetAccountTransactions(ctx, accounts[i].Address(), &history.ReadOptions{Descending: true, Start: lastKey})
			assert.NoError(t, err)
			require.Len(t, entries, 100)
			for e := 0; e < 100; e++ {
				require.True(t, proto.Equal(generated[99-e], entries[e]))
			}

			// Descending, limit
			entries, err = rw.GetAccountTransactions(ctx, accounts[i].Address(), &history.ReadOptions{Descending: true, Start: lastKey, Limit: 50})
			assert.NoError(t, err)
			require.Len(t, entries, 50)
			for e := 0; e < 50; e++ {
				require.True(t, proto.Equal(generated[99-e], entries[e]))
			}
		}
	})
}
