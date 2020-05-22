package invoice

import (
	"context"

	"github.com/pkg/errors"

	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"
)

var (
	ErrExists   = errors.New("invoice list already exists")
	ErrNotFound = errors.New("invoice list not found")
)

type Store interface {
	// Put puts an commonpb.InvoiceList to the store.
	//
	// ErrExists is returned if an invoice list has already been stored
	// for this transaction.
	Put(ctx context.Context, txHash []byte, list *commonpb.InvoiceList) error

	// Get gets the stored commonpb.InvoiceList for a given transaction.
	//
	// ErrNotFound is returned if no commonpb.InvoiceList exists for the
	// transaction.
	Get(ctx context.Context, txHash []byte) (*commonpb.InvoiceList, error)
}
