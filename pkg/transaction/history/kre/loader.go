package kre

import (
	"bytes"
	"context"
	"encoding/base64"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/kinecosystem/agora-common/retry"
	"github.com/kinecosystem/agora-common/retry/backoff"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/system"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/kinecosystem/agora/pkg/transaction/history"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

type Loader struct {
	log       *logrus.Entry
	hist      history.Reader
	committer ingestion.Committer
	lock      ingestion.DistributedLock
	sc        solana.Client
	tc        *token.Client

	paymentsTable  *bigquery.Table
	creationsTable *bigquery.Table
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
	bq *bigquery.Client,
	creationsTable string,
	paymentsTable string,
) *Loader {
	return &Loader{
		log:            logrus.StandardLogger().WithField("type", "transaction/history/kre"),
		hist:           hist,
		committer:      committer,
		lock:           lock,
		sc:             sc,
		tc:             tc,
		creationsTable: bq.Dataset("solana").Table(creationsTable),
		paymentsTable:  bq.Dataset("solana").Table(paymentsTable),
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
						log.WithError(err).Warn("failed to process history")
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
		for _, entry := range entries {
			se := entry.GetSolana()
			if se == nil {
				continue
			}

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

			payments = append(payments, txnPayments...)
			creations = append(creations, txnCreations...)
		}

		//
		// Ship the entries off to BigQuery
		//
		if err := l.paymentsTable.Inserter().Put(context.Background(), payments); err != nil {
			return errors.Wrap(err, "failed to insert payments")
		}

		if err := l.creationsTable.Inserter().Put(context.Background(), creations); err != nil {
			return errors.Wrap(err, "failed to insert creations")
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
			return errors.Wrap(err, "failed to update commit pointer")
		}
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

		var hasCreate bool
		for j := i - 1; j >= 0; j-- {
			createInstr, err := system.DecompileCreateAccount(txn.Message, j)
			if err != nil {
				continue
			}

			if bytes.Equal(createInstr.Address, decompiled.Account) {
				hasCreate = true
				if !bytes.Equal(decompiled.Owner, l.tc.Token()) {
					continue
				}
				break
			}
		}

		// If there was no create instruction for this initialized account,
		// we need to look up the account separately to confirm that it's for
		// out token. We don't do this if the transaction wasn't successful, because
		// this transaction wouldn't have created it, and it saves us a few hops.
		if !hasCreate && successful {
			// note: GetAccount returns an error if the account is not for the configured mint.
			_, err := l.tc.GetAccount(decompiled.Account, solana.CommitmentSingle)
			if err == token.ErrAccountNotFound || err == token.ErrInvalidTokenAccount {
				continue
			} else if err != nil {
				return nil, errors.Wrap(err, "failed to load account")
			}
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
			// able to retrieve the account info, as the it was referenced in a successful
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
