package invoice

import (
	"context"

	"github.com/pkg/errors"

	commonpb "github.com/kinecosystem/kin-api-internal/genproto/common/v3"
)

type Record struct {
	Invoice *commonpb.Invoice
	TxHash []byte
}

var (
	ErrExists   = errors.New("invoice already exists")
	ErrNotFound = errors.New("invoice not found")
)

type Store interface {
	// Add adds an invoice to the store.
	//
	// Returns ErrExists if the invoice already exists in the store
	Add(ctx context.Context, inv *commonpb.Invoice, txHash []byte) error

	// Get gets an invoice record by its invoice hash.
	//
	// Returns ErrNotFound if it could not be found.
	Get(ctx context.Context, invoiceHash []byte) (*Record, error)
}
