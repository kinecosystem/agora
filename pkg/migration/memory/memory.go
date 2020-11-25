package memory

import (
	"context"
	"crypto/ed25519"
	"sync"

	"github.com/kinecosystem/agora/pkg/migration"
)

type store struct {
	sync.Mutex
	entries map[string]migration.State
}

// New returns a memory backed migration.Store
func New() migration.Store {
	return &store{
		entries: make(map[string]migration.State),
	}
}

// Get implements migration.Store.Get.
func (s *store) Get(ctx context.Context, account ed25519.PublicKey) (migration.State, error) {
	s.Lock()
	defer s.Unlock()

	return s.entries[string(account)], nil
}

// Update implements migration.Store.Update.
func (s *store) Update(_ context.Context, account ed25519.PublicKey, prev, next migration.State) error {
	s.Lock()
	defer s.Unlock()

	existing := s.entries[string(account)]
	if existing != prev {
		return migration.ErrStatusMismatch
	}

	s.entries[string(account)] = next
	return nil
}

func (s *store) reset() {
	s.Lock()
	defer s.Unlock()

	s.entries = make(map[string]migration.State)
}
