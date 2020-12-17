package account

import (
	"context"
	"crypto/ed25519"

	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/pkg/errors"
)

var ErrNotFound = errors.New("no mapping")

type Mapper interface {
	Get(ctx context.Context, tokenAddress ed25519.PublicKey, commitment solana.Commitment) (owner ed25519.PublicKey, err error)
	Add(ctx context.Context, tokenAddress ed25519.PublicKey, owner ed25519.PublicKey) error
}

type mapper struct {
	tc    *token.Client
	cache Mapper
}

func NewMapper(tc *token.Client, cache Mapper) Mapper {
	return &mapper{
		tc:    tc,
		cache: cache,
	}
}

func (m *mapper) Get(ctx context.Context, tokenAddress ed25519.PublicKey, commitment solana.Commitment) (owner ed25519.PublicKey, err error) {
	owner, err = m.cache.Get(ctx, tokenAddress, commitment)
	if err != nil && err != ErrNotFound {
		return nil, err
	}
	if err == nil {
		return owner, nil
	}

	info, err := m.tc.GetAccount(tokenAddress, commitment)
	if err == token.ErrAccountNotFound || err == token.ErrInvalidTokenAccount {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}

	if err := m.Add(ctx, tokenAddress, info.Owner); err != nil {
		return nil, errors.Wrap(err, "failed to writeback")
	}
	return info.Owner, nil
}

func (m *mapper) Add(ctx context.Context, tokenAddress ed25519.PublicKey, owner ed25519.PublicKey) error {
	return m.cache.Add(ctx, tokenAddress, owner)
}
