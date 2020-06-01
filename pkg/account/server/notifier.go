package server

import (
	"sync"
	"time"

	"github.com/kinecosystem/go/clients/horizon"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go/xdr"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v3"
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"

	"github.com/kinecosystem/agora/pkg/transaction"
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

func NewAccountNotifier(hClient horizon.ClientInterface) transaction.Notifier {
	return &AccountNotifier{
		log:     logrus.StandardLogger().WithField("type", "account/server/notifier"),
		streams: make(map[string][]*eventStream),
		hClient: hClient,
	}
}

// NewTransaction implements transaction.Notifier.NewTransaction
func (a *AccountNotifier) NewTransaction(e xdr.TransactionEnvelope, m xdr.TransactionMeta) {
	log := a.log.WithField("method", "NewTransaction")
	log.Info(m)

	envBytes, err := e.MarshalBinary()
	if err != nil {
		log.WithError(err).Warn("failed to marshal transaction e, dropping transaction")
		return
	}

	accountIDs := make(map[string]bool)
	txSourceAddr, err := e.Tx.SourceAccount.GetAddress()
	if err != nil {
		log.WithError(err).Warn("failed to get transaction source account address")
	} else {
		accountIDs[txSourceAddr] = true
	}

	for _, op := range e.Tx.Operations {
		if op.SourceAccount != nil {
			addr, err := op.SourceAccount.GetAddress()
			if err != nil {
				log.WithError(err).Warn("failed to get operation source account address")
			}
			accountIDs[addr] = true
		}

		switch op.Body.Type {
		case xdr.OperationTypeCreateAccount:
			addr, err := op.Body.CreateAccountOp.Destination.GetAddress()
			if err != nil {
				log.WithError(err).Warn("failed to get create account destination account address")
			}
			accountIDs[addr] = true
		case xdr.OperationTypePayment:
			addr, err := op.Body.PaymentOp.Destination.GetAddress()
			if err != nil {
				log.WithError(err).Warn("failed to get payment operation destination account address")
			}
			accountIDs[addr] = true
		case xdr.OperationTypeAccountMerge:
			addr, err := op.Body.Destination.GetAddress()
			if err != nil {
				log.WithError(err).Warn("failed to get operation destination account address")
			}
			accountIDs[addr] = true
		}
	}

	accounts := make(map[string]*accountpb.AccountInfo)
	for _, opMeta := range m.OperationsMeta() {
		for _, lec := range opMeta.Changes {
			switch lec.Type {
			case xdr.LedgerEntryChangeTypeLedgerEntryCreated, xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
				entry, ok := lec.GetLedgerEntry()
				if !ok {
					log.Warn("state ledger entry not present in state ledger entry change")
				}

				if entry.Data.Type == xdr.LedgerEntryTypeAccount {
					account, ok := entry.Data.GetAccount()
					if !ok {
						log.Warn("account not present in account ledger entry data")
					}

					accountInfo, err := parseAccountInfoFromEntry(account)
					if err != nil {
						log.WithError(err).Warn("failed to parse account info from account entry")
					}

					accounts[accountInfo.AccountId.Value] = accountInfo
				}
			}
		}
	}

	for accountID := range accountIDs {
		a.streamsMu.Lock()
		streams := a.streams[accountID]
		if len(streams) > 0 {
			events := &accountpb.Events{
				Events: []*accountpb.Event{
					{
						Type: &accountpb.Event_TransactionEvent{
							TransactionEvent: &accountpb.TransactionEvent{
								EnvelopeXdr: envBytes,
							},
						},
					},
				},
			}

			accountInfo, ok := accounts[accountID]
			if !ok {
				log.WithError(err).Info("account info not present in result meta, fetching from Horizon")

				account, err := a.hClient.LoadAccount(accountID)
				if err != nil {
					log.WithError(err).Warnf("failed to get account %s, excluding account event", accountID)
				}

				accountInfo, err = parseAccountInfo(account)
				if err != nil {
					log.WithError(err).Warnf("failed to parse account info for account %s, excluding account event", accountID)
				}
			}

			if accountInfo != nil {
				events.Events = append(events.Events, &accountpb.Event{
					Type: &accountpb.Event_AccountUpdateEvent{
						AccountUpdateEvent: &accountpb.AccountUpdateEvent{
							AccountInfo: accountInfo,
						},
					},
				})
				log.Infof("balance: %d", accountInfo.Balance)
			}

			for _, s := range streams {
				if s != nil {
					err := s.notify(events, notifyTimeout)
					if err != nil {
						log.WithError(err).Warn("failed to notify stream")
					}
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

func parseAccountInfoFromEntry(entry xdr.AccountEntry) (*accountpb.AccountInfo, error) {
	addr, err := entry.AccountId.GetAddress()
	if err != nil {
		return nil, err
	}

	return &accountpb.AccountInfo{
		AccountId:      &commonpb.StellarAccountId{Value: addr},
		SequenceNumber: int64(entry.SeqNum),
		Balance:        int64(entry.Balance),
	}, nil
}
