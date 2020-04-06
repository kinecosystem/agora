package tests

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	commonpb "github.com/kinecosystem/kin-api/genproto/common/v3"

	"github.com/kinecosystem/agora-transaction-services-internal/pkg/data"
)

func RunTests(t *testing.T, store data.Store, teardown func()) {
	for _, tf := range []func(*testing.T, data.Store){testRoundTrip, testCollision} {
		tf(t, store)
		teardown()
	}
}

func testRoundTrip(t *testing.T, store data.Store) {
	t.Run("TestRoundTrip", func(t *testing.T) {
		fk := make([]byte, 32)
		for i := byte(0); i < 32; i++ {
			fk[i] = i
		}

		d := &commonpb.AgoraData{
			Title:           "Test",
			Description:     "abc",
			TransactionType: commonpb.AgoraData_EARN,
			ForeignKey:      fk,
		}

		// Invalid key prefixes
		for _, k := range [][]byte{nil, fk[:28], fk[:30]} {
			actual, err := store.Get(context.Background(), k)
			require.NotNil(t, err)
			require.Nil(t, actual)
		}

		// Doesn't exist yet
		actual, err := store.Get(context.Background(), fk)
		require.Equal(t, data.ErrNotFound, err)
		require.Nil(t, actual)

		require.NoError(t, store.Add(context.Background(), d))

		actual, err = store.Get(context.Background(), fk)
		require.NoError(t, err)
		require.True(t, proto.Equal(d, actual))

		actual, err = store.Get(context.Background(), fk[:29])
		require.NoError(t, err)
		require.True(t, proto.Equal(d, actual))

	})
}

func testCollision(t *testing.T, store data.Store) {
	t.Run("TestCollision", func(t *testing.T) {
		fk := make([]byte, 32)
		for i := byte(0); i < 32; i++ {
			fk[i] = i
		}

		d := &commonpb.AgoraData{
			Title:           "Test",
			Description:     "abc",
			TransactionType: commonpb.AgoraData_EARN,
			ForeignKey:      fk,
		}

		require.NoError(t, store.Add(context.Background(), d))
		require.Equal(t, data.ErrCollision, store.Add(context.Background(), d))
	})
}
