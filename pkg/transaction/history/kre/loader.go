package kre

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"time"

	"github.com/kinecosystem/agora-common/retry"
	"github.com/kinecosystem/agora-common/retry/backoff"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/kinecosystem/agora/pkg/account/solana/accountinfo"
	"github.com/kinecosystem/agora/pkg/transaction/history"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

var (
	stateBalanceNegative = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "loader_state_balance_negative",
		Help:      "Number of times a state has a negative balance",
	})
	getTransactionsSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "agora",
		Name:      "loader_get_transactions_size",
		Help:      "The number of entries retrieved when getting transactions from history",
	})
	lastCommittedBlock = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "agora",
		Name:      "loader_last_committed_block",
		Help:      "The last committed block by the KRE loader",
	})
	processLoopSuccess = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "loader_process_loop_success",
		Help:      "The number of times a process loop was successful",
	})
	processLoopFailure = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "loader_process_loop_failure",
		Help:      "The number of times a process loop failed",
	})
)

type Loader struct {
	log       *logrus.Entry
	hist      history.Reader
	committer ingestion.Committer
	lock      ingestion.DistributedLock
	sc        solana.Client
	tc        *token.Client

	creationsSubmitter Submitter
	paymentsSubmitter  Submitter

	stateStore accountinfo.StateStore
}

func init() {
	if err := registerMetrics(); err != nil {
		logrus.WithError(err).Error("failde to register KRE loader metrics")
	}
}

func GetKREIngestorName() string {
	return "kre_ingestor"
}

func NewLoader(
	hist history.Reader,
	committer ingestion.Committer,
	lock ingestion.DistributedLock,
	sc solana.Client,
	tc *token.Client,
	creationsSubmitter Submitter,
	paymentsSubmitter Submitter,
	stateStore accountinfo.StateStore,
) *Loader {
	return &Loader{
		log:                logrus.StandardLogger().WithField("type", "transaction/history/kre"),
		hist:               hist,
		committer:          committer,
		lock:               lock,
		sc:                 sc,
		tc:                 tc,
		creationsSubmitter: creationsSubmitter,
		paymentsSubmitter:  paymentsSubmitter,
		stateStore:         stateStore,
	}
}

func (l *Loader) Process(ctx context.Context, interval time.Duration) error {
	log := l.log.WithField("method", "Process")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		_, err := retry.Retry(
			func() error {
				return l.lock.Lock(ctx)
			},
			retry.NonRetriableErrors(context.Canceled),
			retry.BackoffWithJitter(backoff.Constant(5*time.Second), 5*time.Second, 0.1),
		)
		if err != nil {
			return err
		}

		for {
			_, err = retry.Retry(
				func() error {
					err := l.process(ctx)
					if err != nil {
						processLoopFailure.Inc()
						log.WithError(err).Warn("failed to process history")
					} else {
						processLoopSuccess.Inc()
					}

					return err
				},
				retry.NonRetriableErrors(context.Canceled),
				retry.BackoffWithJitter(backoff.BinaryExponential(time.Second), interval, 0.1),
			)
			if err != nil {
				if err == context.Canceled {
					break
				}

				// Since the only non-retriable error is cancelled, which we've
				// checked above, this _shouldn't happen.
				l.log.WithError(err).Warn("failed to process history")
			}

			select {
			case <-ctx.Done():
				break
			case <-time.After(interval):
			}
		}

		if err := l.lock.Unlock(); err != nil {
			log.WithError(err).Warn("Failed to release lock")
		}
	}

}

func (l *Loader) process(ctx context.Context) error {
	log := l.log.WithField("method", "process")

	latest, err := l.committer.Latest(ctx, GetKREIngestorName())
	if err != nil {
		return errors.Wrap(err, "failed to get latest kre commit")
	}

	var lastProcessedBlock uint64
	if latest != nil {
		lastProcessedBlock, err = model.BlockFromOrderingKey(latest)
		if err != nil {
			return errors.Wrap(err, "committer contains invalid pointer")
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		//
		// Load unprocessed entries from history
		//
		maxBlock, err := l.sc.GetSlot(solana.CommitmentMax)
		if err != nil {
			return errors.Wrap(err, "failed to get latest committed block")
		}
		entries, err := l.hist.GetTransactions(ctx, lastProcessedBlock, maxBlock, 1024)
		if err != nil {
			return errors.Wrap(err, "failed to load transactions")
		}
		getTransactionsSize.Set(float64(len(entries)))

		log.WithFields(logrus.Fields{
			"last_processed_block": lastProcessedBlock,
			"max_block":            maxBlock,
			"entries":              len(entries),
		}).Info("Processing entries")

		if len(entries) == 0 {
			break
		}

		//
		// Process the loaded entries
		//
		var payments []*payment
		var creations []*creation
		var ownershipChanges []*ownershipChange
		var slot uint64
		for _, entry := range entries {
			se := entry.GetSolana()
			if se == nil {
				continue
			}
			slot = se.Slot

			successful := len(se.TransactionError) == 0
			var txn solana.Transaction
			if err := txn.Unmarshal(se.Transaction); err != nil {
				return errors.Wrap(err, "failed to unmarshal transaction")
			}

			txnPayments, err := l.getPayments(txn, successful)
			if err != nil {
				return errors.Wrap(err, "failed to get payments from transaction")
			}
			txnCreations, err := l.getCreations(txn, successful)
			if err != nil {
				return errors.Wrap(err, "failed to get creations from transaction")
			}

			// If there aren't any payments or creations in this batch, then we can avoid further
			// processing.
			if len(txnPayments) == 0 && len(txnCreations) == 0 {
				log.WithField("txn", base64.StdEncoding.EncodeToString(se.Transaction)).Debug("No payments or creations to process, skipping")
				continue
			}

			memos := l.getMemos(txn)
			blockTime, err := l.sc.GetBlockTime(se.Slot)
			if err != nil {
				return errors.Wrapf(err, "failed to get block time for slot: %d", se.Slot)
			}

			for _, p := range txnPayments {
				p.blockTime = blockTime
				copy(p.txID[:], txn.Signature())
				p.successful = successful
				p.subsidizer = txn.Message.Accounts[0]

				// We use the first memo to proceed us in the transaction.
				for mIdx := len(memos) - 1; mIdx >= 0; mIdx-- {
					if memos[mIdx].offset < p.offset {
						p.memoText = memos[mIdx].text
						p.memo = memos[mIdx].data
						p.appIndex = memos[mIdx].appIndex
						break
					}
				}
			}

			for _, c := range txnCreations {
				c.blockTime = blockTime
				copy(c.txID[:], txn.Signature())
				c.successful = successful
				c.subsidizer = txn.Message.Accounts[0]

				// We use the first memo to proceed us in the transaction.
				for mIdx := len(memos) - 1; mIdx >= 0; mIdx-- {
					if memos[mIdx].offset < c.offset {
						c.memoText = memos[mIdx].text
						c.memo = memos[mIdx].data
						c.appIndex = memos[mIdx].appIndex
						break
					}
				}
			}

			txnOwnershipChanges, err := l.getOwnershipChanges(txn, successful)
			if err != nil {
				return errors.Wrap(err, "failed to get ownership changes from transaction")
			}

			payments = append(payments, txnPayments...)
			creations = append(creations, txnCreations...)
			ownershipChanges = append(ownershipChanges, txnOwnershipChanges...)
		}

		//
		// Ship the entries off to BigQuery
		//
		if len(creations) > 0 {
			if err := l.creationsSubmitter.Submit(context.Background(), creations); err != nil {
				return errors.Wrap(err, "failed to insert creations")
			}
		}
		if len(payments) > 0 {
			if err := l.paymentsSubmitter.Submit(context.Background(), payments); err != nil {
				return errors.Wrap(err, "failed to insert payments")
			}
		}

		// Update affected accounts
		if err := l.updateAccounts(ctx, slot, creations, payments, ownershipChanges); err != nil {
			return errors.Wrap(err, "failed to update accounts")
		}

		//
		// Update the processing state
		//
		lastProcessedBlock = entries[len(entries)-1].GetSolana().Slot
		blockPointer, err := entries[len(entries)-1].GetOrderingKey()
		if err != nil {
			return errors.Wrap(err, "failed to compute ordering key")
		}

		if err := l.committer.Commit(ctx, GetKREIngestorName(), latest, blockPointer); err != nil {
			// There are cases where latest and blockPointer are the same.
			//
			// todo(perf): we can 'add 1' to the blockPointer here, preventing
			//             us from double processing. Since we sleep every 5 minutes, this is fine.
			//             Additionally, if there is a constant stream of transactions, this case
			//             should be rare. We dump a log to validate
			log.WithFields(logrus.Fields{
				"latest":       hex.EncodeToString(latest),
				"blockPointer": hex.EncodeToString(blockPointer),
			}).Warn("Failed to update pointer")
			return errors.Wrap(err, "failed to update commit pointer")
		}
		lastCommittedBlock.Set(float64(lastProcessedBlock))
		latest = blockPointer
	}

	return nil
}

func (l *Loader) getCreations(txn solana.Transaction, successful bool) (creations []*creation, err error) {
	for i := range txn.Message.Instructions {
		decompiled, err := token.DecompileInitializeAccount(txn.Message, i)
		if err != nil {
			continue
		}
		if !bytes.Equal(decompiled.Mint, l.tc.Token()) {
			continue
		}

		creations = append(creations, &creation{
			account:      decompiled.Account,
			accountOwner: decompiled.Owner,
			offset:       i,
		})
	}

	return creations, nil
}

func (l *Loader) getPayments(txn solana.Transaction, successful bool) (payments []*payment, err error) {
	for i := range txn.Message.Instructions {
		decompiled, err := token.DecompileTransferAccount(txn.Message, i)
		if err != nil {
			continue
		}

		destAccount, err := l.tc.GetAccount(decompiled.Destination, solana.CommitmentSingle)
		if err == token.ErrInvalidTokenAccount {
			// The destination account is either not a token account, or it's not for
			// our configured mint
			continue
		} else if err != nil {
			// If we cannot retrieve the destination account, _and_ there's a transaction failure,
			// then it is likely (but not guaranteed) that the transaction failed because the
			// destination does not exist.
			//
			// If, on the other hand, there is _no_ transaction error, then the we should be
			// able to retrieve the account accountState, as the it was referenced in a successful
			// token transfer instruction.
			//
			// In either case, we don't really have enough information to infer which mint
			// the transfer was for. We can only infer whether or not it was an RPC error, or
			// a transaction error.
			if successful {
				return nil, errors.Wrap(err, "failed to get account for non-failed transaction")
			}

			// Transaction failed, so we don't _really_ care here.
			continue
		}

		payments = append(payments, &payment{
			offset:      i,
			source:      decompiled.Source,
			sourceOwner: decompiled.Owner,
			dest:        decompiled.Destination,
			destOwner:   destAccount.Owner,
			quarks:      decompiled.Amount,
		})
	}

	return payments, nil
}

func (l *Loader) getOwnershipChanges(txn solana.Transaction, successful bool) (changes []*ownershipChange, err error) {
	for i := range txn.Message.Instructions {
		decompiled, err := token.DecompileSetAuthority(txn.Message, i)
		if err != nil {
			continue
		}
		if decompiled.Type != token.AuthorityTypeAccountHolder {
			continue
		}

		_, err = l.tc.GetAccount(decompiled.Account, solana.CommitmentSingle)
		if err == token.ErrInvalidTokenAccount {
			continue
		} else if err != nil {
			if successful {
				return nil, errors.Wrap(err, "failed to get account for non-failed transaction")
			}
			continue
		}

		changes = append(changes, &ownershipChange{
			account:  decompiled.Account,
			newOwner: decompiled.NewAuthority,
		})
	}
	return changes, nil
}

func (l *Loader) updateAccounts(ctx context.Context, slot uint64, creations []*creation, payments []*payment, ownershipChanges []*ownershipChange) error {
	log := l.log.WithField("method", "updateAccounts")
	newAccounts := getNewAccounts(creations)
	balanceDiffs := getBalanceDiffs(payments)

	updateMap := make(map[string]*accountinfo.State)
	for account, owner := range newAccounts {
		updateMap[account] = &accountinfo.State{
			Account: []byte(account),
			Owner:   owner,
			Balance: 0,
			Slot:    slot,
		}
	}

	fetchRequired := make(map[string]struct{})
	for account, diff := range balanceDiffs {
		if state, ok := updateMap[account]; ok {
			state.Balance = state.Balance + diff
			continue
		}

		storedState, err := l.stateStore.Get(ctx, []byte(account))
		if err == accountinfo.ErrNotFound {
			fetchRequired[account] = struct{}{}
		} else if err != nil {
			log.WithError(err).Warn("failed to get account state from account state store")
			return errors.Wrap(err, "failed to get account state from account state store")
		} else {
			// Don't overwrite if our slot is behind the stored slot
			if storedState.Slot >= slot {
				continue
			}

			storedState.Balance = storedState.Balance + diff
			storedState.Slot = slot
			updateMap[account] = storedState
			continue
		}

	}

	for _, oc := range ownershipChanges {
		if state, ok := updateMap[string(oc.account)]; ok {
			state.Owner = oc.newOwner
		}

		storedState, err := l.stateStore.Get(ctx, oc.account)
		if err == accountinfo.ErrNotFound {
			fetchRequired[string(oc.account)] = struct{}{}
		} else if err != nil {
			log.WithError(err).Warn("failed to get account state from account state store")
			return errors.Wrap(err, "failed to get account state from account state store")
		} else {
			// Don't overwrite if our slot is behind the stored slot
			if storedState.Slot >= slot {
				continue
			}

			storedState.Owner = oc.newOwner
			storedState.Slot = slot
			updateMap[string(oc.account)] = storedState
			continue
		}
	}

	for _, state := range updateMap {
		if state.Balance < 0 {
			// An incorrect assumption was made above; meter + mark the account for a fetch
			stateBalanceNegative.Inc()
			fetchRequired[string(state.Account)] = struct{}{}
			continue
		}

		err := l.stateStore.Put(ctx, state)
		if err != nil {
			log.WithError(err).Warn("failed to put account state")

			// Ensure entry is deleted since it will be invalid
			err = l.stateStore.Delete(ctx, state.Account)
			if err != nil {
				log.WithError(err).Warn("failed to delete outdated account state")
			}
			return errors.Wrap(err, "failed to update account state")
		}
	}

	for account := range fetchRequired {
		// We get slot prior to getting account to make sure we don't miss an update.
		// If we were to switch the order, there is a risk of missing updates since the
		// fetched slot is more likely to be later than the slot at which the acocunt was fetched.
		committedSlot, err := l.sc.GetSlot(solana.CommitmentMax)
		if err != nil {
			log.WithError(err).Warn("failed to get slot")
			return errors.Wrap(err, "failed to get slot")
		}
		a, err := l.tc.GetAccount(ed25519.PublicKey(account), solana.CommitmentMax)
		if err != nil {
			log.WithError(err).Warn("failed to get account from Solana")
			return errors.Wrap(err, "failed to get account from Solana")
		}

		if err := l.stateStore.Put(ctx, &accountinfo.State{
			Account: ed25519.PublicKey(account),
			Owner:   a.Owner,
			Balance: int64(a.Amount),
			Slot:    committedSlot,
		}); err != nil {
			log.WithError(err).Warn("failed to put account state")
			return errors.Wrap(err, "failed to update account state")
		}
	}

	return nil
}

func getBalanceDiffs(payments []*payment) map[string]int64 {
	diffMap := make(map[string]int64)

	// update map with any payments
	for _, p := range payments {
		diffMap[string(p.source)] -= int64(p.quarks)
		diffMap[string(p.dest)] += int64(p.quarks)
	}

	return diffMap
}

func getNewAccounts(creations []*creation) map[string]ed25519.PublicKey {
	accounts := make(map[string]ed25519.PublicKey)
	for _, c := range creations {
		accounts[string(c.account)] = c.accountOwner
	}

	return accounts
}

func registerMetrics() error {
	if err := prometheus.Register(stateBalanceNegative); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			stateBalanceNegative = e.ExistingCollector.(prometheus.Counter)
		} else {
			return errors.Wrap(err, "failed to register state balance negative counter")
		}
	}

	if err := prometheus.Register(getTransactionsSize); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			getTransactionsSize = e.ExistingCollector.(prometheus.Gauge)
		} else {
			return errors.Wrap(err, "failed to register get transactions size gauge")
		}
	}

	if err := prometheus.Register(lastCommittedBlock); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			lastCommittedBlock = e.ExistingCollector.(prometheus.Gauge)
		} else {
			return errors.Wrap(err, "failed to register last committed block gauge")
		}
	}

	if err := prometheus.Register(processLoopSuccess); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			processLoopSuccess = e.ExistingCollector.(prometheus.Counter)
		} else {
			return errors.Wrap(err, "failed to register process loop success counter")
		}
	}

	if err := prometheus.Register(processLoopFailure); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			processLoopFailure = e.ExistingCollector.(prometheus.Counter)
		} else {
			return errors.Wrap(err, "failed to register process loop failure counter")
		}
	}

	return nil
}
