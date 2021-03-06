package tests

import (
	"context"
	"crypto/ed25519"
	"errors"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/go/strkey"
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
		testGetAccountTransactions,
		testHistory,
		testDoubleInsert_Stellar,
		testDoubleInsert_Solana,
	} {
		tf(t, rw)
		teardown()
	}
}

func testRoundTrip_Stellar(t *testing.T, rw history.ReaderWriter) {
	t.Run("TestRoundTrip_Stellar", func(t *testing.T) {
		ctx := context.Background()
		accounts := testutil.GenerateAccountIDs(t, 10)
		entry, hash := historytestutil.GenerateStellarEntry(t, 1, 1, accounts[0], accounts[1:], nil, nil)

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

		// stellar transactions aren't currently stored in history
		entries, err := rw.GetTransactions(ctx, 0, 1000, 0)
		assert.Empty(t, entries)
		assert.NoError(t, err)

		for i := 1; i < len(accounts); i++ {
			entries, err := rw.GetAccountTransactions(ctx, accounts[i].Address(), nil)
			assert.NoError(t, err)
			require.Len(t, entries, 1)
			require.True(t, proto.Equal(actual, entries[0]))
		}
	})
}

func testDoubleInsert_Stellar(t *testing.T, rw history.ReaderWriter) {
	t.Run("TestDoubleInsert", func(t *testing.T) {
		ctx := context.Background()
		accounts := testutil.GenerateAccountIDs(t, 10)
		entry, hash := historytestutil.GenerateStellarEntry(t, 1, 1, accounts[0], accounts[1:], nil, nil)

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
		err := rw.Write(ctx, mutated)
		require.NotNil(t, err)
		assert.True(t, errors.Is(err, history.ErrInvalidUpdate), err)

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

func testDoubleInsert_Solana(t *testing.T, rw history.ReaderWriter) {
	t.Run("TestDoubleInsert_Solana", func(t *testing.T) {
		ctx := context.Background()
		sender := testutil.GenerateSolanaKeypair(t)
		accounts := testutil.GenerateSolanaKeys(t, 9)

		// Entries in solana are allowed to "advance" in state, since
		// we ingest before they are finalized. However, entries cannot go
		// backwards. Notably:
		//
		//   1. Slot can go up, but not down
		//   2. Confirmed can't be unconfirmed
		//   3. Transaction cannot change
		//   4. Error cannot be unset
		//
		// To simplify our test, we create an entry that's in the 'end state'
		// for each condition, then attempt to roll back each parameter individually,
		// ensuring it's not possible. Note: we cannot test (3), since the transaction
		// is generated from the raw bytes.
		entry, hash := historytestutil.GenerateSolanaEntry(t, 2, true, sender, accounts[1:], nil, nil)
		entry.Kind.(*model.Entry_Solana).Solana.TransactionError = []byte("some error")
		assert.NoError(t, rw.Write(ctx, entry))

		mutated := make([]*model.Entry, 3)

		// Slot cannot go down
		mutated[0] = proto.Clone(entry).(*model.Entry)
		mutated[0].Kind.(*model.Entry_Solana).Solana.Slot = 0

		// Confirmed cannot be unconfirmed
		mutated[1] = proto.Clone(entry).(*model.Entry)
		mutated[1].Kind.(*model.Entry_Solana).Solana.Confirmed = false

		// Error cannot be unset
		mutated[2] = proto.Clone(entry).(*model.Entry)
		mutated[2].Kind.(*model.Entry_Solana).Solana.TransactionError = nil

		for i, m := range mutated {
			assert.False(t, proto.Equal(entry, m))
			require.NotNil(t, rw.Write(ctx, m), i)

			// Ensure that the write did not go through
			actual, err := rw.GetTransaction(ctx, hash)
			require.NoError(t, err)
			assert.True(t, proto.Equal(actual, entry))
			assert.False(t, proto.Equal(actual, m))

			addr := strkey.MustEncode(strkey.VersionByteAccountID, sender.Public().(ed25519.PublicKey))
			entries, err := rw.GetAccountTransactions(ctx, addr, nil)
			assert.NoError(t, err)
			require.Len(t, entries, 1)
			assert.True(t, proto.Equal(entry, entries[0]))
			assert.False(t, proto.Equal(m, entries[0]))
		}
	})
}

func testGetAccountTransactions(t *testing.T, rw history.ReaderWriter) {
	t.Run("TestGetAccountTransactions", func(t *testing.T) {
		ctx := context.Background()
		kp, senderAddr := testutil.GenerateAccountID(t)
		accounts := testutil.GenerateAccountIDs(t, 9)
		solAccounts := testutil.GenerateSolanaKeys(t, 10)

		raw := strkey.MustDecode(strkey.VersionByteSeed, kp.Seed())
		sender := ed25519.NewKeyFromSeed(raw)

		_, err := rw.GetLatestForAccount(ctx, accounts[0].Address())
		assert.Equal(t, err, history.ErrNotFound)

		generated := make([]*model.Entry, 100)
		for i := 0; i < 50; i++ {
			generated[i], _ = historytestutil.GenerateStellarEntry(t, uint64(i-i%2), i, senderAddr, accounts, nil, nil)
			require.NoError(t, rw.Write(ctx, generated[i]))
		}
		for i := 50; i < 100; i++ {
			generated[i], _ = historytestutil.GenerateSolanaEntry(t, uint64(i), true, sender, solAccounts, nil, nil)
			require.NoError(t, rw.Write(ctx, generated[i]))
		}

		latest, err := rw.GetLatestForAccount(ctx, kp.Address())
		assert.NoError(t, err)
		assert.True(t, proto.Equal(generated[99], latest))

		lastKey, err := generated[99].GetOrderingKey()
		require.NoError(t, err)
		orderingKey, err := generated[10].GetOrderingKey()
		require.NoError(t, err)

		// Ascending, default limit
		for i := 1; i < len(accounts); i++ {
			entries, err := rw.GetAccountTransactions(ctx, kp.Address(), nil)
			assert.NoError(t, err)
			require.Equal(t, 100, len(entries))

			for e := 0; e < 100; e++ {
				require.True(t, proto.Equal(generated[e], entries[e]))
			}

			// Ascending, limit
			entries, err = rw.GetAccountTransactions(ctx, kp.Address(), &history.ReadOptions{Limit: 50})
			assert.NoError(t, err)
			require.Len(t, entries, 50, "len: %v", len(entries))
			for e := 0; e < 50; e++ {
				require.True(t, proto.Equal(generated[e], entries[e]))
			}

			// Ascending, offset
			entries, err = rw.GetAccountTransactions(ctx, kp.Address(), &history.ReadOptions{Start: orderingKey})
			assert.NoError(t, err)
			require.Equal(t, 90, len(entries))
			for e := 0; e < 90; e++ {
				require.True(t, proto.Equal(generated[10+e], entries[e]))
			}

			// Descending, default limit
			//
			// We expect no results, since going "backwards" from 0 should return nothing
			entries, err = rw.GetAccountTransactions(ctx, kp.Address(), &history.ReadOptions{Descending: true})
			assert.NoError(t, err)
			require.Len(t, entries, 0)

			// Descending, default limit + offset
			entries, err = rw.GetAccountTransactions(ctx, kp.Address(), &history.ReadOptions{Descending: true, Start: lastKey})
			assert.NoError(t, err)
			require.Len(t, entries, 100)
			for e := 0; e < 100; e++ {
				require.True(t, proto.Equal(generated[99-e], entries[e]))
			}

			// Descending, limit
			entries, err = rw.GetAccountTransactions(ctx, kp.Address(), &history.ReadOptions{Descending: true, Start: lastKey, Limit: 50})
			assert.NoError(t, err)
			require.Len(t, entries, 50)
			for e := 0; e < 50; e++ {
				require.True(t, proto.Equal(generated[99-e], entries[e]))
			}
		}
	})
}

func testHistory(t *testing.T, rw history.ReaderWriter) {
	t.Run("TestHistory", func(t *testing.T) {
		ctx := context.Background()
		sender := testutil.GenerateSolanaKeypair(t)
		accounts := testutil.GenerateSolanaKeys(t, 10)

		expansion := uint64(10_000 / 2)
		generated := make([]*model.Entry, 200)
		for i := 0; i < 200; i++ {
			// We amplify the slot to exploit weakness's within the partitioning scheme of dynamodb.
			// In theory we should put this in the dynamodb test itself, but it's small enough it's ok here.
			generated[i], _ = historytestutil.GenerateSolanaEntry(t, uint64(i)*expansion, i < 150, sender, accounts, nil, nil)
			require.NoError(t, rw.Write(ctx, generated[i]))
		}

		// No max block
		_, err := rw.GetTransactions(ctx, 0, 0, 0)
		assert.Error(t, err)

		// From block > max block
		_, err = rw.GetTransactions(ctx, 10, 1, 0)
		assert.Error(t, err)

		// Limits outside of entries
		entries, err := rw.GetTransactions(ctx, 0, 300*expansion, 300)
		assert.NoError(t, err)
		require.Equal(t, 150, len(entries))

		for i, e := range entries {
			assert.True(t, proto.Equal(e, generated[i]))
		}

		// Query with offset
		offset := generated[10].GetSolana().Slot
		entries, err = rw.GetTransactions(ctx, offset, 300*expansion, 5)
		assert.NoError(t, err)
		assert.Equal(t, 5, len(entries))

		for i, e := range entries {
			assert.True(t, proto.Equal(e, generated[i+10]))
		}

		// Query with maxBlock offset
		//
		// We expect 6 items because the range is inclusive at both ends.
		entries, err = rw.GetTransactions(ctx, offset, generated[15].GetSolana().Slot, 0)
		assert.NoError(t, err)
		assert.Equal(t, 6, len(entries))

		for i, e := range entries {
			assert.True(t, proto.Equal(e, generated[i+10]))
		}
	})
}
