package migration

import (
	"context"
	"crypto/ed25519"
	"errors"
	"time"

	"github.com/kinecosystem/agora-common/solana"
)

// ErrStatusMismatch indicates the expected previous state was not
// what was stored.
var ErrStatusMismatch = errors.New("previous state does not match stored")

// Status is the 'marked' status of a migration.
//
// The source of truth for an account's migration status is completely
// recoverable from the chain, and will be used as a fallback. If the
// status is marked as complete, the migrators will _not_ consult the chain.
type Status int

const (
	// StatusNone indicates either no migration has occurred, or no
	// information about a migration is stored.
	StatusNone Status = iota

	// StatusInProgress indicates that a migration transaction was
	// likely submitted, but its result is unknown.
	StatusInProgress

	// StatusComplete indicates that a migration transaction reached
	// max lockout with certainty.
	StatusComplete
)

// State represents the recorded migration state about an account.
type State struct {
	Status       Status
	Signature    solana.Signature
	LastModified time.Time
}

var ZeroState State

type Store interface {
	// Get returns the recorded state, if any, for an account.
	//
	// ZeroState is returned if there is no persisted state.
	Get(ctx context.Context, account ed25519.PublicKey) (State, error)

	// Update updates the state for an account. The previous state must match
	// what was stored. Implementations must support atomic compare-and-swap.
	Update(ctx context.Context, account ed25519.PublicKey, prev, next State) error
}
