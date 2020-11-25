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
	for _, tf := range []func(*testing.T, migration.Store){testRoundTrip} {
		tf(t, store)
		teardown()
	}
}

func testRoundTrip(t *testing.T, store migration.Store) {
	var prev, next migration.State

	account := testutil.GenerateSolanaKeys(t, 1)[0]

	// Ensure default state
	s, err := store.Get(context.Background(), account)
	assert.NoError(t, err)
	assert.Equal(t, prev, s)

	next.Status = migration.StatusInProgress
	_, err = rand.Reader.Read(next.Signature[:])
	assert.NoError(t, err)

	// Previous mismatch
	err = store.Update(context.Background(), account, next, next)
	require.Equal(t, migration.ErrStatusMismatch, err)

	// Ensure we didn't store the new entry yet
	s, err = store.Get(context.Background(), account)
	assert.NoError(t, err)
	assert.Equal(t, prev, s)

	// Ensure advancing works correctly
	err = store.Update(context.Background(), account, prev, next)
	assert.NoError(t, err)

	s, err = store.Get(context.Background(), account)
	assert.NoError(t, err)
	assert.Equal(t, next, s)
}
