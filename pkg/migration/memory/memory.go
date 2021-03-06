package memory

import (
	"context"
	"crypto/ed25519"
	"sync"

	"github.com/kinecosystem/agora/pkg/migration"
)

type Store struct {
	sync.Mutex
	entries       map[string]migration.State
	requestCounts map[string]int
}

// New returns a memory backed migration.Store
func New() migration.Store {
	return &Store{
		entries:       make(map[string]migration.State),
		requestCounts: make(map[string]int),
	}
}

// Get implements migration.Store.Get.
func (s *Store) Get(ctx context.Context, account ed25519.PublicKey) (state migration.State, exists bool, err error) {
	s.Lock()
	defer s.Unlock()

	entry, ok := s.entries[string(account)]
	return entry, ok, nil
}

// Update implements migration.Store.Update.
func (s *Store) Update(_ context.Context, account ed25519.PublicKey, prev, next migration.State) error {
	s.Lock()
	defer s.Unlock()

	existing := s.entries[string(account)]
	if existing != prev {
		return migration.ErrStatusMismatch
	}

	s.entries[string(account)] = next
	return nil
}

// GetCount implements migration.Store.GetCount
func (s *Store) GetCount(ctx context.Context, account ed25519.PublicKey) (int, error) {
	s.Lock()
	defer s.Unlock()

	if count, ok := s.requestCounts[string(account)]; ok {
		return count, nil
	}

	return 0, nil
}

// IncrementCount implements migration.Store.IncrementCount
func (s *Store) IncrementCount(ctx context.Context, account ed25519.PublicKey) error {
	count := 0
	s.Lock()
	defer s.Unlock()

	if stored, ok := s.requestCounts[string(account)]; ok {
		count = stored
	}
	count += 1

	s.requestCounts[string(account)] = count
	return nil
}

func (s *Store) Reset() {
	s.Lock()
	defer s.Unlock()

	s.entries = make(map[string]migration.State)
}
