package accountinfo

import (
	"context"
	"crypto/ed25519"

	"github.com/pkg/errors"
)

var (
	ErrNotFound = errors.New("account info not found")
)

type State struct {
	Account ed25519.PublicKey
	Owner   ed25519.PublicKey
	Balance int64
	Slot    uint64
}

type StateStore interface {
	// Put puts an account's state into the store.
	Put(ctx context.Context, state *State) error

	// Get gets an account's state, if it exists in the store.
	//
	// ErrNotFound is returned if no account state was found for the provided key.
	Get(ctx context.Context, account ed25519.PublicKey) (*State, error)

	// Delete deletes an account's state.
	//
	// Delete is idempotent
	Delete(ctx context.Context, account ed25519.PublicKey) error

	// Returns token accounts by owner
	//
	// If no token accounts are associated with the owner, an empty slice will be returned.
	GetAccountsByOwner(ctx context.Context, owner ed25519.PublicKey) ([]ed25519.PublicKey, error)
}
