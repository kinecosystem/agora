package server

import (
	"sync"

	"github.com/kinecosystem/go/clients/horizon"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go/xdr"

	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

type AccountNotifier struct {
	log *logrus.Entry

	streamsMu sync.Mutex
	streams   map[string][]*eventStream

	hClient horizon.ClientInterface
}

func NewAccountNotifier(hClient horizon.ClientInterface) *AccountNotifier {
	return &AccountNotifier{
		log:     logrus.StandardLogger().WithField("type", "account/server/notifier"),
		streams: make(map[string][]*eventStream),
		hClient: hClient,
	}
}

// OnTransaction implements transaction.Notifier.OnTransaction
func (a *AccountNotifier) OnTransaction(e xdr.TransactionEnvelope, r xdr.TransactionResult, m xdr.TransactionMeta) {
	log := a.log.WithField("method", "OnTransaction")

	accountIDs, err := model.GetAccountsFromEnvelope(e)
	if err != nil {
		log.WithError(err).Warn("Failed to get accounts from envelope, dropping notification")
		return
	}

	for accountID := range accountIDs {
		a.streamsMu.Lock()
		streams := a.streams[accountID]

		if len(streams) == 0 {
			a.streamsMu.Unlock()
			continue
		}

		for _, s := range streams {
			if s != nil {
				err := s.notify(e, r, m)
				if err != nil {
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
