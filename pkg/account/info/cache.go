package info

import (
	"context"
	"crypto/ed25519"

	"github.com/pkg/errors"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v4"
)

var (
	ErrAccountInfoNotFound = errors.New("account info not found")
)

type Cache interface {
	// Put puts a list of account info associated with the given owner.
	Put(ctx context.Context, info *accountpb.AccountInfo) error

	// Get gets an account's info, if it exists in the cache.
	//
	// ErrAccountInfoNotFound is returned if no account info was found for the provided key.
	Get(ctx context.Context, key ed25519.PublicKey) (*accountpb.AccountInfo, error)

	// Delete a cached entry.
	Del(ctx context.Context, key ed25519.PublicKey) (ok bool, err error)
}
