package memory

import (
	"context"
	"sync"

	kinkeypair "github.com/kinecosystem/go/keypair"

	"github.com/kinecosystem/agora/pkg/keypair"
)

const StoreType = "memory"

type store struct {
	sync.Mutex
	keypairMap map[string]*kinkeypair.Full
}

func init() {
	keypair.RegisterStoreConstructor(StoreType, newStore)
}

// newStore returns an in-memory implementation of keypair.Keystore
func newStore() (keypair.Keystore, error) {
	return &store{
		keypairMap: make(map[string]*kinkeypair.Full),
	}, nil
}

// Put implements Keystore.Put
func (s *store) Put(ctx context.Context, id string, full *kinkeypair.Full) error {
	s.Lock()
	defer s.Unlock()

	if _, ok := s.keypairMap[id]; ok {
		return keypair.ErrKeypairAlreadyExists
	}

	s.keypairMap[id] = full
	return nil
}

// Get implements Keystore.Get
func (s *store) Get(ctx context.Context, id string) (*kinkeypair.Full, error) {
	s.Lock()
	defer s.Unlock()

	full, ok := s.keypairMap[id]
	if !ok {
		return nil, keypair.ErrKeypairNotFound
	}
	return full, nil
}

func (s *store) resetStore() {
	s.Lock()
	defer s.Unlock()

	s.keypairMap = make(map[string]*kinkeypair.Full)
}
