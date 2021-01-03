package memory

import (
	"bytes"
	"context"
	"sort"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/kinecosystem/agora/pkg/transaction/history"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

type orderedEntries []*model.Entry

func (a orderedEntries) Len() int {
	return len(a)
}
func (a orderedEntries) Swap(i int, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a orderedEntries) Less(i int, j int) bool {
	iKey, err := a[i].GetOrderingKey()
	if err != nil {
		panic(err)
	}
	jKey, err := a[j].GetOrderingKey()
	if err != nil {
		panic(err)
	}

	return bytes.Compare(iKey, jKey) < 0
}

type RW struct {
	sync.Mutex
	Writes      []*model.Entry
	txns        map[string]*model.Entry
	txnHistory  orderedEntries
	accountTxns map[string]orderedEntries
}

func New() *RW {
	return &RW{
		txns:        make(map[string]*model.Entry),
		accountTxns: make(map[string]orderedEntries),
	}
}

// Write implements history.Writer.Write.
func (rw *RW) Write(_ context.Context, e *model.Entry) error {
	rw.Lock()
	defer rw.Unlock()

	hash, err := e.GetTxID()
	if err != nil {
		return err
	}

	accounts, err := e.GetAccounts()
	if err != nil {
		return err
	}

	if prev, ok := rw.txns[string(hash)]; ok {
		if prev.Version < model.KinVersion_KIN4 {
			if !proto.Equal(prev, e) {
				return errors.Wrap(history.ErrInvalidUpdate, "double insert mismatch detected")
			}
		} else {
			prevSol := prev.GetSolana()
			sol := e.GetSolana()

			// If a slot was already stored, it cannot be mutated.
			if prevSol.Slot > 0 && prevSol.Slot != sol.Slot {
				return errors.Wrap(history.ErrInvalidUpdate, "double insert with different entries detected (slot)")
			}

			// A confirmed block cannot be unconfirmed
			if prevSol.Confirmed && !sol.Confirmed {
				return errors.Wrap(history.ErrInvalidUpdate, "double insert with different entries detected (confirmation status)")
			}

			if !bytes.Equal(prevSol.Transaction, sol.Transaction) {
				return errors.Wrap(history.ErrInvalidUpdate, "double insert with different entries detected (transaction)")
			}

			// In theory an error can occur after submission.
			// Therefore, we only check equality if there's an error already set.
			if len(prevSol.TransactionError) > 0 && !bytes.Equal(prevSol.TransactionError, sol.TransactionError) {
				return errors.Wrap(history.ErrInvalidUpdate, "double insert with different entries detected (transaction error)")
			}
		}
	} else {
		rw.txns[string(hash)] = proto.Clone(e).(*model.Entry)
	}

	if solanaEntry := e.GetSolana(); solanaEntry != nil && solanaEntry.Confirmed {
		rw.txnHistory = append(rw.txnHistory, proto.Clone(e).(*model.Entry))
		sort.Sort(rw.txnHistory)
	}

	for _, a := range accounts {
		accountHistory := rw.accountTxns[a]

		var exists bool
		for _, existing := range accountHistory {
			if proto.Equal(existing, e) {
				exists = true
				break
			}
		}
		if !exists {
			accountHistory := append(accountHistory, proto.Clone(e).(*model.Entry))
			sort.Sort(accountHistory)
			rw.accountTxns[a] = accountHistory
		}
	}

	rw.Writes = append(rw.Writes, proto.Clone(e).(*model.Entry))

	return nil
}

// GetTransaction implements history.Writer.GetTransaction.
func (rw *RW) GetTransaction(_ context.Context, txHash []byte) (*model.Entry, error) {
	rw.Lock()
	defer rw.Unlock()

	e, ok := rw.txns[string(txHash)]
	if !ok {
		return nil, history.ErrNotFound
	}
	return e, nil
}

// GetTransactions implements history.Reader.GetTransactions.
func (rw *RW) GetTransactions(_ context.Context, startKey, endKey []byte, limit int) ([]*model.Entry, error) {
	if limit <= 0 {
		limit = 100
	}

	_, err := model.BlockFromOrderingKey(startKey)
	if err != nil && err != model.ErrInvalidOrderingKeyVersion {
		return nil, errors.Wrap(err, "invalid start key")
	}
	endBlock, err := model.BlockFromOrderingKey(endKey)
	if err == model.ErrInvalidOrderingKeyVersion {
		return nil, nil
	} else if err != nil {
		return nil, errors.Wrap(err, "invalid end key")
	}

	if endBlock == 0 {
		return nil, errors.New("maxBlock must be non-zero")
	}
	if bytes.Compare(startKey, endKey) >= 0 {
		return nil, errors.New("startKey must be < endKey")
	}

	rw.Lock()
	defer rw.Unlock()

	// We could use sort.Search() here, but we don't expect to
	// use the memory implementation for anything outside of testing,
	// so we expect small values of len(rw.txnHistory)
	copy := make([]*model.Entry, 0, len(rw.txnHistory))
	for i := 0; i < len(rw.txnHistory); i++ {
		orderingKey, err := rw.txnHistory[i].GetOrderingKey()
		if err != nil {
			return nil, errors.Wrap(err, "failed to get ordering key")
		}

		if bytes.Compare(orderingKey, startKey) < 0 {
			continue
		}
		if bytes.Compare(orderingKey, endKey) >= 0 {
			break
		}

		copy = append(copy, proto.Clone(rw.txnHistory[i]).(*model.Entry))
		if len(copy) >= limit {
			break
		}
	}

	return copy, nil
}

// GetLatestForAccount implements history.Reader.GetLatestForAccount.
func (rw *RW) GetLatestForAccount(_ context.Context, account string) (*model.Entry, error) {
	rw.Lock()
	defer rw.Unlock()

	accountHistory := rw.accountTxns[account]
	if len(accountHistory) == 0 {
		return nil, history.ErrNotFound
	}

	return proto.Clone(accountHistory[len(accountHistory)-1]).(*model.Entry), nil
}

// GetAccountTransactions implements history.Writer.GetAccountTransactions.
func (rw *RW) GetAccountTransactions(_ context.Context, account string, opts *history.ReadOptions) ([]*model.Entry, error) {
	rw.Lock()
	defer rw.Unlock()

	accountHistory := rw.accountTxns[account]
	if len(accountHistory) == 0 {
		return accountHistory, nil
	}

	i := sort.Search(len(accountHistory), func(i int) bool {
		orderingKey, err := accountHistory[i].GetOrderingKey()
		if err != nil {
			panic(err)
		}

		return bytes.Compare(orderingKey, opts.GetStart()) >= 0
	})

	limit := opts.GetLimit()
	if limit <= 0 {
		limit = 100
	}

	var results []*model.Entry
	if opts.GetDescending() {
		for ; i >= 0; i-- {
			orderingKey, err := accountHistory[i].GetOrderingKey()
			if err != nil {
				return nil, errors.Wrap(err, "failed to get ordering key")
			}

			// Descending has an edge case when the starting key is "older" than
			// the first entry in the list, since sort.Search returns the index
			// that the search entry is (if it exists), or should be inserted.
			if bytes.Compare(orderingKey, opts.GetStart()) > 0 {
				continue
			}

			results = append(results, proto.Clone(accountHistory[i]).(*model.Entry))
			if len(results) == limit {
				break
			}
		}
	} else {
		for ; i < len(accountHistory); i++ {
			results = append(results, proto.Clone(accountHistory[i]).(*model.Entry))
			if len(results) == limit {
				break
			}
		}
	}

	return results, nil
}

// Reset resets the recorded writes.
func (rw *RW) Reset() {
	rw.Lock()
	defer rw.Unlock()

	rw.Writes = nil
	rw.txnHistory = nil
	rw.accountTxns = make(map[string]orderedEntries)
	rw.txns = make(map[string]*model.Entry)
}
