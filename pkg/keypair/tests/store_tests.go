package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	kinkeypair "github.com/kinecosystem/go/keypair"

	"github.com/kinecosystem/agora/pkg/keypair"
)

const TestKeypairID = "someid"

type testCase func(t *testing.T, testStore keypair.Keystore, teardown func())

func RunKeystoreTests(t *testing.T, store keypair.Keystore, teardown func()) {
	for _, test := range []testCase{testPutThenGet, testPutExists, testGetNotFound} {
		test(t, store, teardown)
	}
}

func testPutThenGet(t *testing.T, testStore keypair.Keystore, teardown func()) {
	t.Run("testPutThenGet", func(t *testing.T) {
		defer teardown()

		kp, err := kinkeypair.Random()
		require.NoError(t, err)

		err = testStore.Put(context.Background(), TestKeypairID, kp)
		require.NoError(t, err)

		storedKeypair, err := testStore.Get(context.Background(), TestKeypairID)
		require.NoError(t, err)

		require.Equal(t, kp, storedKeypair)
	})
}

func testPutExists(t *testing.T, testStore keypair.Keystore, teardown func()) {
	t.Run("testPutExists", func(t *testing.T) {
		defer teardown()

		kp, err := kinkeypair.Random()
		require.NoError(t, err)

		err = testStore.Put(context.Background(), TestKeypairID, kp)
		require.NoError(t, err)

		err = testStore.Put(context.Background(), TestKeypairID, kp)
		require.Equal(t, keypair.ErrKeypairAlreadyExists, err)
	})
}

func testGetNotFound(t *testing.T, testStore keypair.Keystore, teardown func()) {
	t.Run("TestGetNotFound", func(t *testing.T) {
		defer teardown()

		storedKeypair, err := testStore.Get(context.Background(), TestKeypairID)
		require.Equal(t, keypair.ErrKeypairNotFound, err)
		require.Nil(t, storedKeypair)
	})
}
