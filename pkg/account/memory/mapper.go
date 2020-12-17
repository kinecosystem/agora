package memory

import (
	"context"
	"crypto/ed25519"
	"sync"

	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora/pkg/account"
)

type mapper struct {
	sync.Mutex
	m map[string]string
}

func New() account.Mapper {
	return &mapper{
		m: make(map[string]string),
	}
}

func (m *mapper) Add(_ context.Context, tokenAccount, owner ed25519.PublicKey) error {
	m.Lock()
	defer m.Unlock()

	m.m[string(tokenAccount)] = string(owner)
	return nil
}

func (m *mapper) Get(_ context.Context, tokenAccount ed25519.PublicKey, _ solana.Commitment) (ed25519.PublicKey, error) {
	m.Lock()
	defer m.Unlock()

	owner, ok := m.m[string(tokenAccount)]
	if !ok {
		return nil, account.ErrNotFound
	}

	return ed25519.PublicKey(owner), nil
}

func (m *mapper) reset() {
	m.Lock()
	defer m.Unlock()

	m.m = make(map[string]string)
}
