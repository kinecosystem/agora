package server

import (
	"sync"

	"github.com/kinecosystem/agora-common/solana"
	"github.com/sirupsen/logrus"

	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

type AccountNotifier struct {
	log *logrus.Entry

	streamsMu sync.Mutex
	streams   map[string][]*eventStream
}

func NewAccountNotifier() *AccountNotifier {
	return &AccountNotifier{
		log:     logrus.StandardLogger().WithField("type", "account/notifier"),
		streams: make(map[string][]*eventStream),
	}
}

// OnTransaction implements transaction.Notifier.OnTransaction
func (a *AccountNotifier) OnTransaction(txn solana.BlockTransaction) {
	log := a.log.WithField("method", "OnTransaction")

	accountIDs, err := model.GetAccountsFromTransaction(txn.Transaction)
	if err != nil {
		log.WithError(err).Warn("failed to get accounts from transaction; dropping notification")
		return
	}

	for accountID := range accountIDs {
		a.streamsMu.Lock()
		streams := a.streams[accountID]

		if len(a.streams) == 0 {
			a.streamsMu.Unlock()
			continue
		}

		for _, s := range streams {
			if s != nil {
				if err := s.notify(txn); err != nil {
					log.WithError(err).Warn("failed to notify stream")
				}
			}
		}

		a.streamsMu.Unlock()
	}
}

// AddStream adds a stream to the notifier.
func (a *AccountNotifier) AddStream(accountID string, stream *eventStream) {
	a.streamsMu.Lock()
	a.streams[accountID] = append(a.streams[accountID], stream)
	a.streamsMu.Unlock()
}

// RemoveStream removes a stream from the notifier.
func (a *AccountNotifier) RemoveStream(accountID string, stream *eventStream) {
	a.streamsMu.Lock()
	defer a.streamsMu.Unlock()

	streamIdx := -1
	for idx, s := range a.streams[accountID] {
		if s == stream {
			streamIdx = idx
			break
		}
	}

	if streamIdx == -1 {
		return
	}

	a.streams[accountID] = append(a.streams[accountID][:streamIdx], a.streams[accountID][streamIdx+1:]...)
}
