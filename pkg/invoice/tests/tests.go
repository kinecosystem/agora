package tests

import (
	"context"
	"crypto/sha256"
	"math/rand"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/kinecosystem/agora-common/kin"
	"github.com/stretchr/testify/require"

	commonpb "github.com/kinecosystem/kin-api/genproto/common/v3"

	"github.com/kinecosystem/agora-transaction-services/pkg/invoice"
)

func RunTests(t *testing.T, store invoice.Store, teardown func()) {
	for _, tf := range []func(*testing.T, invoice.Store){testRoundTrip, testCollision} {
		tf(t, store)
		teardown()
	}
}

func testRoundTrip(t *testing.T, store invoice.Store) {
	t.Run("TestRoundTrip", func(t *testing.T) {
		txHash := sha256.Sum256([]byte("somedata"))
		txKey := txHash[:]

		inv := &commonpb.Invoice{
			Items: []*commonpb.Invoice_LineItem{
				{
					Title:       "lineitem1",
					Description: "desc1",
					Amount:      5,
					Sku:         nil,
				},
				{
					Title:       "lineitem2",
					Description: "desc3",
					Amount:      2,
					Sku:         nil,
				},
			},
			Nonce: &commonpb.Nonce{
				GenerationTime: ptypes.TimestampNow(),
				Value:          rand.Int63(),
			},
		}

		prefix, err := invoice.GetHashPrefix(inv)
		require.NoError(t, err)

		// Doesn't exist yet
		actual, err := store.Get(context.Background(), prefix, txKey)
		require.Equal(t, invoice.ErrNotFound, err)
		require.Nil(t, actual)

		require.NoError(t, store.DoesNotExist(context.Background(), inv))

		require.NoError(t, store.Add(context.Background(), inv, txKey))

		b, err := proto.Marshal(inv)
		require.NoError(t, err)
		h := sha256.Sum256(b)
		memo, err := kin.NewMemo(byte(0), kin.TransactionTypeSpend, 1, h[:29])

		require.NoError(t, err)
		actual, err = store.Get(context.Background(), memo.ForeignKey(), txKey)
		require.NoError(t, err)
		require.True(t, proto.Equal(inv, actual))

		err = store.DoesNotExist(context.Background(), inv)
		require.Equal(t, invoice.ErrExists, err)
	})
}

func testCollision(t *testing.T, store invoice.Store) {
	t.Run("TestCollision", func(t *testing.T) {
		txHash := sha256.Sum256([]byte("somedata"))
		txKey := txHash[:]

		inv := &commonpb.Invoice{
			Items: []*commonpb.Invoice_LineItem{
				{
					Title:       "lineitem1",
					Description: "desc1",
					Amount:      5,
					Sku:         nil,
				},
				{
					Title:       "lineitem2",
					Description: "desc3",
					Amount:      2,
					Sku:         nil,
				},
			},
			Nonce: &commonpb.Nonce{
				GenerationTime: ptypes.TimestampNow(),
				Value:          rand.Int63(),
			},
		}

		require.NoError(t, store.Add(context.Background(), inv, txKey))

		err := store.Add(context.Background(), inv, txKey)
		require.Equal(t, invoice.ErrExists, err)
	})
}
