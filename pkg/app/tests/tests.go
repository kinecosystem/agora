package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora-transaction-services-internal/pkg/app"
)

func RunTests(t *testing.T, store app.ConfigStore, teardown func()) {
	for _, tf := range []func(*testing.T, app.ConfigStore){testRoundTrip, testInvalidParameters} {
		tf(t, store)
		teardown()
	}
}

func testRoundTrip(t *testing.T, store app.ConfigStore) {
	t.Run("testRoundTrip", func(t *testing.T) {
		actualConfig, err := store.Get(context.Background(), 0)
		require.Equal(t, app.ErrNotFound, err)
		require.Nil(t, actualConfig)

		config := &app.Config{
			AppName:            "kin",
			AgoraDataURL:       "test.kin.org/agoradata",
			SignTransactionURL: "test.kin.org/signtx",
		}

		err = store.Add(context.Background(), 0, config)
		require.NoError(t, err)

		err = store.Add(context.Background(), 0, config)
		require.Error(t, err, app.ErrExists)

		actualConfig, err = store.Get(context.Background(), 0)
		require.NoError(t, err)
		require.Equal(t, config, actualConfig)
	})
}

func testInvalidParameters(t *testing.T, store app.ConfigStore) {
	t.Run("testInvalidParameters", func(t *testing.T) {
		err := store.Add(context.Background(), 0, nil)
		require.Error(t, err)

		err = store.Add(context.Background(), 0, &app.Config{})
		require.Error(t, err)
	})
}
