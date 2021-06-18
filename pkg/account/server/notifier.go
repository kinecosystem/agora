package server

import (
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/sirupsen/logrus"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v4"
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v4"

	"github.com/kinecosystem/agora/pkg/events/eventspb"
	"github.com/kinecosystem/agora/pkg/solanautil"
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

func (a *AccountNotifier) OnEvent(e *eventspb.Event) {
	log := a.log.WithField("method", "OnEvent")

	var txn solana.Transaction
	var event *accountpb.Event

	switch t := e.Kind.(type) {
	case *eventspb.Event_TransactionEvent:
		var txErr *commonpb.TransactionError
		if len(t.TransactionEvent.TransactionError) > 0 {
			txErr = &commonpb.TransactionError{}
			if err := proto.Unmarshal(t.TransactionEvent.TransactionError, txErr); err != nil {
				log.WithError(err).Warn("failed to unmarshal transaction error from event, dropping")
				return
			}
		}

		event = &accountpb.Event{
			Type: &accountpb.Event_TransactionEvent{
				TransactionEvent: &accountpb.TransactionEvent{
					Transaction: &commonpb.Transaction{
						Value: t.TransactionEvent.Transaction,
					},
					TransactionError: txErr,
				},
			},
		}

		if err := txn.Unmarshal(t.TransactionEvent.Transaction); err != nil {
			log.WithError(err).Warn("failed to unmarshal transaction from event, dropping")
			return
		}
	case *eventspb.Event_SimulationEvent:
		var txErr *commonpb.TransactionError
		if len(t.SimulationEvent.TransactionError) > 0 {
			txErr = &commonpb.TransactionError{}
			if err := proto.Unmarshal(t.SimulationEvent.TransactionError, txErr); err != nil {
				log.WithError(err).Warn("failed to unmarshal transaction error from event, dropping")
				return
			}
		}

		event = &accountpb.Event{
			Type: &accountpb.Event_SimulationEvent{
				SimulationEvent: &accountpb.SimulationEvent{
					Transaction: &commonpb.Transaction{
						Value: t.SimulationEvent.Transaction,
					},
					TransactionError: txErr,
				},
			},
		}

		if err := txn.Unmarshal(t.SimulationEvent.Transaction); err != nil {
			log.WithError(err).Warn("failed to unmarshal transaction from event, dropping")
			return
		}
	default:
		log.Warn("unknown event type; dropping")
		return
	}

	accountIDs, err := model.GetAccountsFromTransaction(txn)
	if err != nil {
		log.WithError(err).Warn("failed to get accounts from transaction (via simulation); dropping notification")
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
				if err := s.notify(event); err != nil {
					log.WithError(err).Warn("failed to notify stream")
				}
			}
		}

		a.streamsMu.Unlock()
	}
}

// OnTransaction implements transaction.Notifier.OnTransaction
func (a *AccountNotifier) OnTransaction(txn solana.BlockTransaction) {
	log := a.log.WithField("method", "OnTransaction")

	var err error
	var txErr *commonpb.TransactionError
	if txn.Err != nil {
		txErr, err = solanautil.MapTransactionError(*txn.Err)
		if err != nil {
			log.WithError(err).Warn("failed to map transaction error from simulation")
			raw, err := txn.Err.JSONString()
			if err != nil {
				log.WithError(err).Warn("failed to get transaction error JSON string")
			}

			txErr = &commonpb.TransactionError{
				Reason: commonpb.TransactionError_UNKNOWN,
				Raw:    []byte(raw),
			}
		}
	}

	event := &accountpb.Event{
		Type: &accountpb.Event_TransactionEvent{
			TransactionEvent: &accountpb.TransactionEvent{
				Transaction: &commonpb.Transaction{
					Value: txn.Transaction.Marshal(),
				},
				TransactionError: txErr,
			},
		},
	}

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
				if err := s.notify(event); err != nil {
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
