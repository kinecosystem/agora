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

	"github.com/kinecosystem/agora-transaction-services-internal/pkg/invoice"
)

func RunTests(t *testing.T, store invoice.Store, teardown func()) {
	for _, tf := range []func(*testing.T, invoice.Store){testRoundTrip, testExists} {
		tf(t, store)
		teardown()
	}
}

func testRoundTrip(t *testing.T, store invoice.Store) {
	t.Run("TestRoundTrip", func(t *testing.T) {
		h := sha256.Sum256([]byte("somedata"))
		txHash := h[:]

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

		invoiceHash, err := invoice.GetHash(inv)
		require.NoError(t, err)

		// Doesn't exist yet
		record, err := store.Get(context.Background(), invoiceHash)
		require.Equal(t, invoice.ErrNotFound, err)
		require.Nil(t, record)

		require.NoError(t, store.Add(context.Background(), inv, txHash))

		b, err := proto.Marshal(inv)
		require.NoError(t, err)
		fk := sha256.Sum224(b)
		memo, err := kin.NewMemo(byte(0), kin.TransactionTypeSpend, 1, fk[:])

		require.NoError(t, err)
		record, err = store.Get(context.Background(), memo.ForeignKey()[:28])
		require.NoError(t, err)
		require.True(t, proto.Equal(inv, record.Invoice))
		require.Equal(t, txHash, record.TxHash)
	})
}

func testExists(t *testing.T, store invoice.Store) {
	t.Run("TestCollision", func(t *testing.T) {
		h := sha256.Sum256([]byte("somedata"))
		txHash := h[:]

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

		require.NoError(t, store.Add(context.Background(), inv, txHash))

		err := store.Add(context.Background(), inv, txHash)
		require.Equal(t, invoice.ErrExists, err)
	})
}
