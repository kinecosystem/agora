package server

import (
	"sync"
	"time"

	"github.com/kinecosystem/go/clients/horizon"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go/xdr"
)

const (
	notifyTimeout = 10 * time.Second
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
func (a *AccountNotifier) OnTransaction(e xdr.TransactionEnvelope, m xdr.TransactionMeta) {
	log := a.log.WithField("method", "OnTransaction")

	accountIDs := a.getUniqueAccounts(e)

	for accountID := range accountIDs {
		a.streamsMu.Lock()
		streams := a.streams[accountID]

		if len(streams) == 0 {
			a.streamsMu.Unlock()
			continue
		}

		for _, s := range streams {
			if s != nil {
				err := s.notify(e, m, notifyTimeout)
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

func (a *AccountNotifier) getUniqueAccounts(e xdr.TransactionEnvelope) map[string]struct{} {
	log := a.log.WithField("method", "getUniqueAccounts")

	accountIDs := make(map[string]struct{})
	txSourceAddr, err := e.Tx.SourceAccount.GetAddress()
	if err != nil {
		log.WithError(err).Warn("failed to get transaction source account address")
	} else {
		accountIDs[txSourceAddr] = struct{}{}
	}

	for _, op := range e.Tx.Operations {
		if op.SourceAccount != nil {
			addr, err := op.SourceAccount.GetAddress()
			if err != nil {
				log.WithError(err).Warn("failed to get operation source account address")
			}
			accountIDs[addr] = struct{}{}
		}

		switch op.Body.Type {
		case xdr.OperationTypeCreateAccount:
			addr, err := op.Body.CreateAccountOp.Destination.GetAddress()
			if err != nil {
				log.WithError(err).Warn("failed to get create account destination account address")
			}
			accountIDs[addr] = struct{}{}
		case xdr.OperationTypePayment:
			addr, err := op.Body.PaymentOp.Destination.GetAddress()
			if err != nil {
				log.WithError(err).Warn("failed to get payment operation destination account address")
			}
			accountIDs[addr] = struct{}{}
		case xdr.OperationTypeAccountMerge:
			addr, err := op.Body.Destination.GetAddress()
			if err != nil {
				log.WithError(err).Warn("failed to get operation destination account address")
			}
			accountIDs[addr] = struct{}{}
		}
	}

	return accountIDs
}
