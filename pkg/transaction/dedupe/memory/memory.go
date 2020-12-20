package memory

import (
	"context"
	"sync"

	"github.com/kinecosystem/agora/pkg/transaction/dedupe"
)

type deduper struct {
	mu sync.Mutex
	m  map[string]dedupe.Info
}

func New() dedupe.Deduper {
	return &deduper{
		m: make(map[string]dedupe.Info),
	}
}

func (d *deduper) Dedupe(ctx context.Context, id []byte, info *dedupe.Info) (*dedupe.Info, error) {
	if len(id) == 0 {
		return nil, nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	prev, ok := d.m[string(id)]
	if ok {
		clone := prev
		return &clone, nil
	}

	d.m[string(id)] = *info
	return nil, nil
}

func (d *deduper) Update(ctx context.Context, id []byte, info *dedupe.Info) error {
	if len(id) == 0 {
		return nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	d.m[string(id)] = *info
	return nil
}

func (d *deduper) Delete(ctx context.Context, id []byte) error {
	if len(id) == 0 {
		return nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.m, string(id))
	return nil
}

func (d *deduper) reset() {
	d.mu.Lock()
	d.m = make(map[string]dedupe.Info)
	d.mu.Unlock()
}
