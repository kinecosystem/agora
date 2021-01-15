package processor

import (
	"bytes"
	"context"
	"encoding/base64"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/agora-common/metrics"
	"github.com/kinecosystem/agora-common/retry"
	"github.com/kinecosystem/agora-common/retry/backoff"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/memo"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/mr-tron/base58/base58"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go/support/log"

	"github.com/kinecosystem/agora/pkg/transaction/history"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

var (
	lastCommittedBlock = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "agora",
		Name:      "kre_loader_last_committed_block",
		Help:      "The last committed block by the KRE loader",
	}, []string{"ingestor"})
	getTransactionsSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "agora",
		Name:      "processor_get_transactions_size",
		Help:      "The number of entries retrieved when getting transactions from history",
	}, []string{"ingestor"})
	processLoopSuccess = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "history_process_loop_success",
		Help:      "The number of times a process loop was successful",
	}, []string{"ingestor"})
	processLoopFailure = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "history_process_loop_failure",
		Help:      "The number of times a process loop failed",
	}, []string{"ingestor"})
)

func init() {
	lastCommittedBlock = metrics.Register(lastCommittedBlock).(*prometheus.GaugeVec)
	getTransactionsSize = metrics.Register(getTransactionsSize).(*prometheus.GaugeVec)
	processLoopSuccess = metrics.Register(processLoopSuccess).(*prometheus.CounterVec)
	processLoopFailure = metrics.Register(processLoopFailure).(*prometheus.CounterVec)
}

type ProcessorCallback func(history.StateChange) error

type Processor struct {
	log          *logrus.Entry
	reader       history.Reader
	committer    ingestion.Committer
	lock         ingestion.DistributedLock
	ingestorName string
	tc           *token.Client
	batchSize    int
	callbacks    []ProcessorCallback
}

func NewProcessor(
	reader history.Reader,
	committer ingestion.Committer,
	lock ingestion.DistributedLock,
	ingestorName string,
	tc *token.Client,
	batchSize int,
	callbacks ...ProcessorCallback,
) *Processor {
	return &Processor{
		log:          logrus.StandardLogger().WithField("type", "history/processor"),
		reader:       reader,
		committer:    committer,
		lock:         lock,
		ingestorName: ingestorName,
		tc:           tc,
		batchSize:    batchSize,
		callbacks:    callbacks,
	}
}

func (p *Processor) Process(ctx context.Context, interval time.Duration) error {
	log := p.log.WithField("method", "Process")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		_, err := retry.Retry(
			func() error {
				return p.lock.Lock(ctx)
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
					err := p.process(ctx)
					if err != nil {
						processLoopFailure.WithLabelValues(p.ingestorName).Inc()
						log.WithError(err).Warn("failed to process history")
					} else {
						processLoopSuccess.WithLabelValues(p.ingestorName).Inc()
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
				p.log.WithError(err).Warn("failed to process history")
			}

			select {
			case <-ctx.Done():
				break
			case <-time.After(interval):
			}
		}

		if err := p.lock.Unlock(); err != nil {
			log.WithError(err).Warn("Failed to release lock")
		}
	}
}

func (p *Processor) process(ctx context.Context) error {
	log := p.log.WithField("method", "process")

	lastProcessed, err := p.committer.Latest(ctx, p.ingestorName)
	if err != nil {
		return errors.Wrap(err, "failed to get latest kre commit")
	}
	if len(lastProcessed) == 0 {
		lastProcessed = model.OrderingKeyFromBlock(0)
	}

	lastIngested, err := p.committer.Latest(ctx, ingestion.GetHistoryIngestorName(model.KinVersion_KIN4))
	if err != nil {
		return errors.Wrap(err, "failed to get last ingested block")
	}
	maxKey := append(lastIngested, 0)

	for bytes.Compare(lastProcessed, append(lastIngested, 0)) < 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		startKey := append(lastProcessed, 0)
		state, err := p.ProcessRange(ctx, startKey, maxKey, p.batchSize)
		if err != nil {
			return errors.Wrap(err, "failed to process range")
		}

		if len(state.Creations) > 0 || len(state.Payments) > 0 || len(state.OwnershipChanges) > 0 {
			for _, cb := range p.callbacks {
				if err := cb(state); err != nil {
					return errors.Wrap(err, "failed to perform callback")
				}
			}
		}

		if state.LastKey == nil {
			break
		}

		if err := p.committer.Commit(ctx, p.ingestorName, lastProcessed, state.LastKey); err != nil {
			log.WithFields(logrus.Fields{
				"prev_key": base64.StdEncoding.EncodeToString(lastProcessed),
				"new_key":  base64.StdEncoding.EncodeToString(state.LastKey),
			}).Warn("Failed to update pointer")
			return errors.Wrap(err, "failed to update commit pointer")
		}
		lastProcessedBlock, err := model.BlockFromOrderingKey(state.LastKey)
		if err != nil {
			return errors.Wrap(err, "failed to get block from prevKey")
		}
		lastCommittedBlock.WithLabelValues(p.ingestorName).Set(float64(lastProcessedBlock))
		lastProcessed = state.LastKey
	}

	return nil
}

// Process processes history over the range of [start, end), yielding all of the state
// changes over that range
func (p *Processor) ProcessRange(ctx context.Context, startKey, endKey []byte, limit int) (sc history.StateChange, err error) {
	log := p.log.WithField("method", "process")

	//
	// Load unprocessed entries from history
	//
	entries, err := p.reader.GetTransactions(ctx, startKey, endKey, limit)
	if err != nil {
		return sc, errors.Wrap(err, "failed to load transactions")
	}
	getTransactionsSize.WithLabelValues(p.ingestorName).Set(float64(len(entries)))

	log.WithFields(logrus.Fields{
		"start_key":   base64.StdEncoding.EncodeToString(startKey),
		"start_block": model.MustBlockFromOrderingKey(startKey),
		"end_key":     base64.StdEncoding.EncodeToString(endKey),
		"end_block":   model.MustBlockFromOrderingKey(endKey),
		"entries":     len(entries),
	}).Debug("Processing entries")
	if len(entries) == 0 {
		return sc, nil
	}

	//
	// Process the loaded entries
	//
	for _, entry := range entries {
		se := entry.GetSolana()
		if se == nil {
			continue
		}

		successful := len(se.TransactionError) == 0
		var txn solana.Transaction
		if err := txn.Unmarshal(se.Transaction); err != nil {
			return sc, errors.Wrap(err, "failed to unmarshal transaction")
		}

		memos := p.getMemos(txn)
		blockTime, err := ptypes.Timestamp(se.BlockTime)
		if err != nil {
			return sc, errors.Wrap(err, "failed to create timestamppb")
		}
		if blockTime.IsZero() || blockTime.Unix() == 0 {
			return sc, errors.Errorf("missing block time at block: %d", se.Slot)
		}

		txnPayments, err := p.getPayments(txn, successful)
		if err != nil {
			return sc, errors.Wrap(err, "failed to get payments from transaction")
		}
		txnCreations, err := p.getCreations(txn, successful)
		if err != nil {
			return sc, errors.Wrap(err, "failed to get creations from transaction")
		}
		txnOwnershipChanges, err := p.getOwnershipChanges(txn, successful)
		if err != nil {
			return sc, errors.Wrap(err, "failed to get ownership changes from transaction")
		}

		applyOwnershipChanges(txnCreations, txnOwnershipChanges)

		// If there aren't any payments or creations in this batch, then we can avoid further
		// processing.
		if len(txnPayments) == 0 && len(txnCreations) == 0 && len(txnOwnershipChanges) == 0 {
			log.WithField("txn", base64.StdEncoding.EncodeToString(se.Transaction)).Debug("No payments or creations to process, skipping")
			continue
		}

		for _, p := range txnPayments {
			p.Block = se.Slot
			p.BlockTime = blockTime
			copy(p.TxID[:], txn.Signature())
			p.Successful = successful
			p.Subsidizer = txn.Message.Accounts[0]

			// We use the first memo to proceed us in the transaction.
			for mIdx := len(memos) - 1; mIdx >= 0; mIdx-- {
				if memos[mIdx].Offset < p.Offset {
					p.MemoText = memos[mIdx].Text
					p.Memo = memos[mIdx].Data
					p.AppIndex = memos[mIdx].AppIndex
					break
				}
			}
		}

		for _, c := range txnCreations {
			c.Block = se.Slot
			c.BlockTime = blockTime
			copy(c.TxID[:], txn.Signature())
			c.Successful = successful
			c.Subsidizer = txn.Message.Accounts[0]

			// We use the first memo to proceed us in the transaction.
			for mIdx := len(memos) - 1; mIdx >= 0; mIdx-- {
				if memos[mIdx].Offset < c.Offset {
					c.MemoText = memos[mIdx].Text
					c.Memo = memos[mIdx].Data
					c.AppIndex = memos[mIdx].AppIndex
					break
				}
			}
		}

		sc.Payments = append(sc.Payments, txnPayments...)
		sc.Creations = append(sc.Creations, txnCreations...)
		sc.OwnershipChanges = append(sc.OwnershipChanges, txnOwnershipChanges...)
	}

	if len(entries) > 0 {
		sc.LastKey, err = entries[len(entries)-1].GetOrderingKey()
		if err != nil {
			return sc, errors.Wrap(err, "failed to get ordering key")
		}
	}

	return sc, nil
}

func (p *Processor) getCreations(txn solana.Transaction, successful bool) (creations []*history.Creation, err error) {
	for i := range txn.Message.Instructions {
		decompiled, err := token.DecompileInitializeAccount(txn.Message, i)
		if err != nil {
			continue
		}
		if !bytes.Equal(decompiled.Mint, p.tc.Token()) {
			continue
		}

		creations = append(creations, &history.Creation{
			Account:      decompiled.Account,
			AccountOwner: decompiled.Owner,
			Offset:       i,
		})
	}

	return creations, nil
}

func (p *Processor) getPayments(txn solana.Transaction, successful bool) (payments []*history.Payment, err error) {
	for i := range txn.Message.Instructions {
		decompiled, err := token.DecompileTransferAccount(txn.Message, i)
		if err != nil {
			continue
		}

		destAccount, err := p.tc.GetAccount(decompiled.Destination, solana.CommitmentSingle)
		if err == token.ErrInvalidTokenAccount {
			// The destination account is either not a token account, or it's not for
			// our configured mint
			continue
		} else if err == token.ErrAccountNotFound {
			log.WithField("account", base58.Encode(decompiled.Destination)).Warn("account not found while processing payment, ignoring")
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

		payments = append(payments, &history.Payment{
			Offset:      i,
			Source:      decompiled.Source,
			SourceOwner: decompiled.Owner,
			Dest:        decompiled.Destination,
			DestOwner:   destAccount.Owner,
			Quarks:      decompiled.Amount,
		})
	}

	return payments, nil
}

func (p *Processor) getOwnershipChanges(txn solana.Transaction, successful bool) (changes []*history.OwnershipChange, err error) {
	for i := range txn.Message.Instructions {
		decompiled, err := token.DecompileSetAuthority(txn.Message, i)
		if err != nil {
			continue
		}
		if decompiled.Type != token.AuthorityTypeAccountHolder {
			continue
		}

		_, err = p.tc.GetAccount(decompiled.Account, solana.CommitmentSingle)
		if err == token.ErrInvalidTokenAccount {
			continue
		} else if err != nil {
			if successful {
				return nil, errors.Wrap(err, "failed to get account for non-failed transaction")
			}
			continue
		}

		changes = append(changes, &history.OwnershipChange{
			Address: decompiled.Account,
			Owner:   decompiled.NewAuthority,
		})
	}
	return changes, nil
}
func (p *Processor) getMemos(txn solana.Transaction) (memos []history.MemoData) {
	for i := range txn.Message.Instructions {
		decompiled, err := memo.DecompileMemo(txn.Message, i)
		if err != nil {
			continue
		}
		if len(decompiled.Data) == 0 {
			continue
		}

		m := history.MemoData{
			Data:   decompiled.Data,
			Offset: i,
		}
		if strings.HasPrefix(string(m.Data), "1-") {
			str := string(m.Data)
			m.Text = &str
		}

		if raw, err := base64.StdEncoding.DecodeString(string(m.Data)); err == nil {
			var km kin.Memo
			copy(km[:], raw)

			if kin.IsValidMemoStrict(km) {
				m.AppIndex = int(km.AppIndex())
			}
		}

		memos = append(memos, m)
	}

	return memos
}

func applyOwnershipChanges(creations []*history.Creation, changes []*history.OwnershipChange) {
	// note: the number of creations in a transaction is generally small. for a
	//       migration transaction we can fit at most two, and the SDK flow does
	//       just 1. The O(n^2) nature of this loop should still be small, 2^2 in
	//       the worst case
	for _, change := range changes {
		for _, c := range creations {
			if bytes.Equal(c.Account, change.Address) {
				c.AccountOwner = change.Owner
			}
		}
	}
}
