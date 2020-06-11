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

type ReadOptions struct {
	Descending bool
	Start      []byte
	Limit      int
}

func (r *ReadOptions) GetDescending() bool {
	if r == nil {
		return false
	}

	return r.Descending
}

func (r *ReadOptions) GetStart() []byte {
	if r == nil || len(r.Start) == 0 {
		return []byte{0}
	}

	return r.Start
}

func (r *ReadOptions) GetLimit() int {
	if r == nil {
		return 0
	}

	return r.Limit
}

type Reader interface {
	// GetTransaction returns the model.Entry associated with the specified txHash.
	//
	// ErrNotFound is returned if no entry has been written.
	GetTransaction(ctx context.Context, txHash []byte) (*model.Entry, error)

	// GetAccountTransactions returns the model.Entry's with the specified options.
	//
	// If no options are specified, the results will be in ascending order starting
	// from beginning (with an undefined limit).
	GetAccountTransactions(ctx context.Context, account string, opts *ReadOptions) ([]*model.Entry, error)
}

type ReaderWriter interface {
	Reader
	Writer
}
