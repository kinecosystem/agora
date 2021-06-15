package server

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"github.com/kinecosystem/agora-common/retry"
	"github.com/kinecosystem/agora-common/retry/backoff"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/kinecosystem/agora/pkg/events"
	"github.com/kinecosystem/agora/pkg/events/eventspb"
)

type Notifier interface {
	OnTransaction(solana.BlockTransaction)
}

func StreamTransactions(ctx context.Context, client solana.Client, notifiers ...Notifier) {
	log := logrus.StandardLogger().WithFields(logrus.Fields{
		"type":   "transaction/solana/stream",
		"method": "StreamTransactions",
	})

	// We need to determine some kind of seed / starting point to begin emitting events for.
	//
	// From there, we can simply poll at some slot generation interval. Alternatively, we can
	// open a websocket connection, but we've been told it's unreliable.
	var seed uint64

	err := retry.Loop(
		func() (err error) {
			if seed == 0 {
				seed, err = client.GetSlot(solana.CommitmentMax)
				if err != nil {
					log.WithError(err).Warn("failed to get seed slot")
					return err
				}

				blocks, err := client.GetConfirmedBlocksWithLimit(seed, 1024)
				if err != nil {
					log.WithError(err).Warn("failed to get blocks from seed slot")
					return err
				}

				if len(blocks) == 0 {
					// todo: handle better
					return errors.Errorf("no blocks from seed: %d", seed)
				}

				seed = blocks[len(blocks)-1]
			}

			log.WithField("seed", seed).Info("starting event loop")

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			blocks, err := client.GetConfirmedBlocksWithLimit(seed, 1024)
			if err != nil {
				log.WithError(err).Warn("failed to get blocks")
				return err
			}

			for _, b := range blocks {
				block, err := client.GetConfirmedBlock(b)
				if err != nil {
					log.WithError(err).Warnf("failed to get block: %v", b)
					return err
				}

				for _, txn := range block.Transactions {
					for _, n := range notifiers {
						n.OnTransaction(txn)
					}
				}
			}

			newSeed := seed
			if len(blocks) > 0 {
				newSeed = blocks[len(blocks)-1]
			}

			log.WithFields(logrus.Fields{
				"old_seed": seed,
				"new_seed": newSeed,
			}).Info("finished block process")
			seed = newSeed

			time.Sleep(solana.PollRate)
			return nil
		},
		retry.NonRetriableErrors(context.Canceled),
		retry.BackoffWithJitter(backoff.BinaryExponential(time.Second), 10*time.Second, 0.5),
	)
	if err == context.Canceled {
		log.WithError(err).Info("transaction stream terminated")
	} else {
		log.WithError(err).Warn("transaction stream terminated")
	}
}

func MapTransactionEvent(n Notifier) events.Hook {
	log := logrus.StandardLogger().WithFields(logrus.Fields{
		"type":   "transaction/solana/stream",
		"method": "MapTransactionEvent",
	})

	return func(e *eventspb.Event) {
		txEvent := e.GetTransactionEvent()
		if txEvent == nil {
			return
		}

		var b solana.BlockTransaction
		if err := b.Transaction.Unmarshal(txEvent.Transaction); err != nil {
			log.WithError(err).Warn("failed to unmarshal event, dropping")
			return
		}

		if len(txEvent.TransactionError) > 0 {
			var txError interface{}
			err := json.NewDecoder(bytes.NewBuffer(txEvent.TransactionError)).Decode(&txError)
			if err != nil {
				log.WithError(err).Warn("failed to unmarshal event error, dropping")
				return
			}

			b.Err, err = solana.ParseTransactionError(txError)
			if err != nil {
				log.WithError(err).Warn("failed to parse event error, dropping")
				return
			}
		}

		n.OnTransaction(b)
	}
}
