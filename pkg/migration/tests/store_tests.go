package tests

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/migration"
	"github.com/kinecosystem/agora/pkg/testutil"
)

func RunStoreTests(t *testing.T, store migration.Store, teardown func()) {
	for _, tf := range []func(*testing.T, migration.Store){testRoundTrip, testCount} {
		tf(t, store)
		teardown()
	}
}

func testRoundTrip(t *testing.T, store migration.Store) {
	var prev, next migration.State

	account := testutil.GenerateSolanaKeys(t, 1)[0]

	// Ensure default state
	s, exists, err := store.Get(context.Background(), account)
	assert.NoError(t, err)
	assert.Equal(t, prev, s)
	assert.False(t, exists)

	next.Status = migration.StatusInProgress
	_, err = rand.Reader.Read(next.Signature[:])
	assert.NoError(t, err)

	// Previous mismatch
	err = store.Update(context.Background(), account, next, next)
	require.Equal(t, migration.ErrStatusMismatch, err)

	// Ensure we didn't store the new entry yet
	s, exists, err = store.Get(context.Background(), account)
	assert.NoError(t, err)
	assert.Equal(t, prev, s)
	assert.False(t, exists)

	// Ensure advancing works correctly
	err = store.Update(context.Background(), account, prev, next)
	assert.NoError(t, err)

	s, exists, err = store.Get(context.Background(), account)
	assert.NoError(t, err)
	assert.Equal(t, next, s)
	assert.True(t, exists)

}

func testCount(t *testing.T, store migration.Store) {
	account := testutil.GenerateSolanaKeys(t, 1)[0]

	count, err := store.GetCount(context.Background(), account)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	require.NoError(t, store.IncrementCount(context.Background(), account))
	require.NoError(t, store.IncrementCount(context.Background(), account))
	require.NoError(t, store.IncrementCount(context.Background(), account))

	count, err = store.GetCount(context.Background(), account)
	require.NoError(t, err)
	assert.Equal(t, 3, count)
}
