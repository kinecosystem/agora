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

	// Get gets an invoice by its invoice hash and transaction hash.
	//
	// Returns ErrNotFound if it could not be found.
	Get(ctx context.Context, invoiceHash []byte, txHash []byte) (*commonpb.Invoice, error)

	// Exists returns whether an invoice with the provided hash exists in the store.
	Exists(ctx context.Context, invoiceHash []byte) (bool, error)
}
