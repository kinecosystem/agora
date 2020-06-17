package history

import (
	"context"
	"errors"

	model "github.com/kinecosystem/agora/pkg/transaction/history/model"
)

var (
	ErrNotFound = errors.New("not found")
)

type Writer interface {
	// Write writes a model.Entry to a history store.
	Write(context.Context, *model.Entry) error
}

type Reader interface {
	// GetTransaction returns the model.Entry associated with the specified txHash.
	//
	// ErrNotFound is returned if no entry has been written.
	GetTransaction(ctx context.Context, txHash []byte) (*model.Entry, error)

	// GetAccountTransactions returns the model.Entry's for a specified account between
	// the provided range [start, end).
	GetAccountTransactions(ctx context.Context, account string, start, end []byte) ([]*model.Entry, error)
}

type ReaderWriter interface {
	Reader
	Writer
}
