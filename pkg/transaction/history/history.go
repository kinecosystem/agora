package history

import (
	"context"
	"errors"

	model "github.com/kinecosystem/agora/pkg/transaction/history/model"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrInvalidUpdate = errors.New("invalid update")
)

type Writer interface {
	// Write writes a model.Entry to a history store.
	//
	// ErrInvalidUpdate is returned if the entry would overwrite
	// an existing entry in an invalid way. For example, revert the
	// transaction state to an earlier one.
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

	// GetTransactions returns an ordered set of transactions over the range of
	// [fromBlock, maxBlock]
	//
	// If no limit is provided, a default limit of 100 is used.
	//
	// In order to bound / terminate queries, maxBlock must be set. It should be set
	// to the last known confirmed block if no other value is desired.
	GetTransactions(ctx context.Context, fromBlock, maxBlock uint64, limit int) ([]*model.Entry, error)

	// GetAccountTransactions returns the model.Entry's with the specified options.
	//
	// If no options are specified, the results will be in ascending order starting
	// from the beginning (with an undefined limit).
	GetAccountTransactions(ctx context.Context, account string, opts *ReadOptions) ([]*model.Entry, error)

	// GetLatestForAccount returns the latest model.Entry in an accounts history.
	//
	// The entry is not guaranteed to be committed. As a result, it should be filtered
	// before being returned to callers / users.
	//
	// If no entries exist for the account, ErrNotFound is returned.
	GetLatestForAccount(ctx context.Context, account string) (*model.Entry, error)
}

type ReaderWriter interface {
	Reader
	Writer
}
