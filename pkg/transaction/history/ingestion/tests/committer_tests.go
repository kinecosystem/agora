package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
)

func RunCommitterTests(t *testing.T, c ingestion.Committer, teardown func()) {
	for _, tf := range []func(*testing.T, ingestion.Committer){testCommitter_HappyPath} {
		tf(t, c)
		teardown()
	}
}

func testCommitter_HappyPath(t *testing.T, c ingestion.Committer) {
	t.Run("TestCommitter_HappyPath", func(t *testing.T) {
		ctx := context.Background()
		for i := byte(0); i < 10; i++ {
			var p ingestion.Pointer
			if i > 0 {
				p = []byte{i - 1}
			}

			block := ingestion.Pointer([]byte{i})
			assert.NoError(t, c.Commit(ctx, "kin2", p, block))

			Latest, err := c.Latest(ctx, "kin2")
			assert.NoError(t, err)
			assert.Equal(t, Latest, block)
		}

		// Ensure only kin2 ingestor was updated
		Latest, err := c.Latest(ctx, "kin3")
		assert.NoError(t, err)
		assert.Nil(t, Latest)

		// Attempt a gap
		assert.Equal(t, ingestion.ErrInvalidCommit, c.Commit(ctx, "kin2", []byte{12}, []byte{13}))

		// Attempt a correct Latest, but backwards commit
		assert.Equal(t, ingestion.ErrInvalidCommit, c.Commit(ctx, "kin2", []byte{9}, []byte{8}))

		// Noop commit should be ok.
		assert.Equal(t, ingestion.ErrInvalidCommit, c.Commit(ctx, "kin2", []byte{9}, []byte{9}))
	})
}
