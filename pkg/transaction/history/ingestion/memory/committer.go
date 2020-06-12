package memory

import (
	"bytes"
	"context"
	"sync"

	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
)

type committer struct {
	sync.Mutex
	ptrs map[string]ingestion.Pointer
}

func New() ingestion.Committer {
	return &committer{
		ptrs: make(map[string]ingestion.Pointer),
	}
}

// Commit implements ingestion.Committer.Commit.
func (c *committer) Commit(_ context.Context, name string, parent ingestion.Pointer, block ingestion.Pointer) error {
	c.Lock()
	defer c.Unlock()

	cloned := make(ingestion.Pointer, len(block))
	copy(cloned, block)

	if p, ok := c.ptrs[name]; ok {
		if !bytes.Equal(p, parent) || bytes.Compare(parent, block) > 0 {
			return ingestion.ErrInvalidCommit
		}
	}

	c.ptrs[name] = block
	return nil
}

// Latest implements ingestion.Committer.Latest.
func (c *committer) Latest(ctx context.Context, name string) (ingestion.Pointer, error) {
	c.Lock()
	defer c.Unlock()

	p, ok := c.ptrs[name]
	if !ok {
		return nil, nil
	}

	cloned := make(ingestion.Pointer, len(p))
	copy(cloned, p)

	return cloned, nil
}
