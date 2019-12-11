package data

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kinecosystem/kin-api/genproto/common/v3"
)

var (
	ErrNotFound  = errors.New("agora data not found")
	ErrCollision = errors.New("agora fk collision")
)

// Store stores off-chain agora transaction data.
type Store interface {
	// Add adds agora data to the store.
	//
	// Returns an ErrCollision if data already exists for the specified foreign
	// key prefix.
	//
	// todo: potentially support full FK matching.
	Add(context.Context, *common.AgoraData) error

	// Get gets the agora data for either a prefix (29 bytes) or a full FK (30 bytes).
	//
	// Returns ErrNotFound if no data exists.
	//
	// todo: should potentally be doing stronger validation on prefix (230-bit mask).
	// todo: API is likely breaking if we support full FK resolution, since prefix is multi-return.
	Get(ctx context.Context, prefixOrKey []byte) (*common.AgoraData, error)
}
