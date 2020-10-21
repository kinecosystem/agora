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

type accountHistory []*model.Entry

func (a accountHistory) Len() int {
	return len(a)
}
func (a accountHistory) Swap(i int, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a accountHistory) Less(i int, j int) bool {
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
	accountTxns map[string]accountHistory
}

func New() *RW {
	return &RW{
		txns:        make(map[string]*model.Entry),
		accountTxns: make(map[string]accountHistory),
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
				return errors.New("double insert mismatch detected")
			}
		} else {
			prevSol := prev.GetSolana()
			sol := e.GetSolana()

			// If a slot was already stored, it cannot be mutated.
			if prevSol.Slot > 0 && prevSol.Slot != sol.Slot {
				return errors.New("double insert with different entries detected")
			}

			// A confirmed block cannot be unconfirmed
			if prevSol.Confirmed && !sol.Confirmed {
				return errors.New("double insert with different entries detected")
			}

			if !bytes.Equal(prevSol.Transaction, sol.Transaction) {
				return errors.New("double insert with different entries detected")
			}

			// In theory an error can occur after submission.
			// Therefore, we only check equality if there's an error already set.
			if len(prevSol.TransactionError) > 0 && !bytes.Equal(prevSol.TransactionError, sol.TransactionError) {
				return errors.New("double insert with different entries detected")
			}
		}
	} else {
		rw.txns[string(hash)] = proto.Clone(e).(*model.Entry)
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

// Reeset resets the recored writes.
func (rw *RW) Reset() {
	rw.Lock()
	defer rw.Unlock()

	rw.Writes = nil
}
