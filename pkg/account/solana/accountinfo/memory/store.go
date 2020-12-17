package memory

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"sync"

	"github.com/kinecosystem/agora/pkg/account/solana/accountinfo"
	"github.com/pkg/errors"
)

type store struct {
	sync.RWMutex
	store map[string]*accountinfo.State
}

func NewStore() accountinfo.StateStore {
	return &store{
		store: make(map[string]*accountinfo.State),
	}
}

func (s *store) Put(ctx context.Context, state *accountinfo.State) error {
	if state.Account == nil || state.Owner == nil || state.Slot == 0 {
		return errors.New("account, owner and slot must all be set")
	}

	s.Lock()
	defer s.Unlock()

	s.store[string(state.Account)] = state

	return nil
}

func (s *store) Get(ctx context.Context, account ed25519.PublicKey) (*accountinfo.State, error) {
	s.RLock()
	defer s.RUnlock()

	if entry, ok := s.store[string(account)]; ok {
		return entry, nil
	}

	return nil, accountinfo.ErrNotFound
}

func (s *store) Delete(ctx context.Context, account ed25519.PublicKey) error {
	s.Lock()
	defer s.Unlock()

	delete(s.store, string(account))

	return nil
}

func (s *store) GetAccountsByOwner(ctx context.Context, owner ed25519.PublicKey) ([]ed25519.PublicKey, error) {
	s.RLock()
	defer s.RUnlock()

	accounts := make([]ed25519.PublicKey, 0)

	// This isn't great but this impl is for testing
	for _, state := range s.store {
		if bytes.Equal(owner, state.Owner) {
			accounts = append(accounts, state.Account)
		}
	}

	return accounts, nil
}

func (s *store) reset() {
	s.store = make(map[string]*accountinfo.State)
}
