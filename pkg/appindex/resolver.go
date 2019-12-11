package appindex

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/kin-api/genproto/common/v3"
)

var (
	ErrNotFound = errors.New("app index mapping not found")
)

// Resolver resolves the endpoint that can be queried for memo data
// for a given memo. If the resolver cannot determine the endpoint,
// ErrNotFound is returned.
type Resolver interface {
	Resolve(context.Context, kin.Memo) (*common.AgoraDataUrl, error)
}
