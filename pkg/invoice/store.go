package invoice

import (
	"context"

	"github.com/pkg/errors"

	commonpb "github.com/kinecosystem/kin-api/genproto/common/v3"
)

var (
	ErrExists   = errors.New("invoice already exists")
	ErrNotFound = errors.New("invoice not found")
)

type Store interface {
	// Add adds an invoice to the store.
	//
	// Returns ErrExists if the invoice already exists in the store
	Add(ctx context.Context, inv *commonpb.Invoice, txHash []byte) error

	// Get gets an invoice by its hash prefix and transaction hash.
	//
	// Returns ErrNotFound if it could not be found.
	Get(ctx context.Context, prefix []byte, txHash []byte) (*commonpb.Invoice, error)

	// PrefixExists verifies that the provided invoice does not already exist in the store.
	PrefixExists(ctx context.Context, prefix []byte) (bool, error)
}
