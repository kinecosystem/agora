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

	rw.Writes = append(rw.Writes, proto.Clone(e).(*model.Entry))

	hash, err := e.GetTxHash()
	if err != nil {
		return err
	}

	accounts, err := e.GetAccounts()
	if err != nil {
		return err
	}

	if _, ok := rw.txns[string(hash)]; !ok {
		rw.txns[string(hash)] = proto.Clone(e).(*model.Entry)
	}

	for _, a := range accounts {
		accountHistory := append(rw.accountTxns[a], proto.Clone(e).(*model.Entry))
		sort.Sort(accountHistory)
		rw.accountTxns[a] = accountHistory
	}

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
