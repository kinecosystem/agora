package tests

import (
	"context"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/agora-api/genproto/transaction/v4"
	"github.com/kinecosystem/agora/pkg/transaction/dedupe"
	"github.com/stretchr/testify/assert"
)

func RunTests(t *testing.T, d dedupe.Deduper, teardown func()) {
	for _, tf := range []func(t *testing.T, d dedupe.Deduper){
		testRoundTrip,
		testNoID,
	} {
		tf(t, d)
		teardown()
	}
}

func testRoundTrip(t *testing.T, d dedupe.Deduper) {
	t.Run("testRoundTrip", func(t *testing.T) {
		ctx := context.Background()
		id := []byte("hello")

		info := &dedupe.Info{
			Signature:      []byte("sig"),
			SubmissionTime: time.Unix(time.Now().Unix(), 0),
		}

		prev, err := d.Dedupe(ctx, id, info)
		assert.NoError(t, err)
		assert.Nil(t, prev)

		prev, err = d.Dedupe(ctx, id, &dedupe.Info{
			Signature: []byte("sig2"),
		})
		assert.NoError(t, err)
		assert.EqualValues(t, info, prev)

		info.Response = &transaction.SubmitTransactionResponse{
			Result: transaction.SubmitTransactionResponse_FAILED,
		}
		assert.NoError(t, d.Update(ctx, id, info))

		prev, err = d.Dedupe(ctx, id, &dedupe.Info{
			Signature: []byte("sig2"),
		})
		assert.NoError(t, err)
		assert.Equal(t, info.SubmissionTime, prev.SubmissionTime)
		assert.Equal(t, info.Signature, prev.Signature)
		assert.True(t, proto.Equal(info.Response, prev.Response))
	})
}

func testNoID(t *testing.T, d dedupe.Deduper) {
	t.Run("testNoID", func(t *testing.T) {
		ctx := context.Background()

		info := &dedupe.Info{
			Signature: []byte("sig"),
		}

		for _, id := range [][]byte{nil, {}} {
			prev, err := d.Dedupe(ctx, id, info)
			assert.NoError(t, err)
			assert.Nil(t, prev)

			assert.NoError(t, d.Update(ctx, id, info))

			prev, err = d.Dedupe(ctx, id, info)
			assert.NoError(t, err)
			assert.Nil(t, prev)

			assert.NoError(t, d.Delete(ctx, id))
		}
	})
}
