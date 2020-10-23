package solana

import (
	"context"
	"time"

	"github.com/kinecosystem/agora-common/retry"
	"github.com/kinecosystem/agora-common/retry/backoff"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/sirupsen/logrus"
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
				seed, err = client.GetSlot()
				if err != nil {
					log.WithError(err).Warn("failed to get seed slot")
					return err
				}

				blocks, err := client.GetConfirmedBlocksWithLimit(seed-30, 1024)
				if err != nil {
					log.WithError(err).Warn("failed to get blocks from seed slot")
					return err
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

			log.WithFields(logrus.Fields{
				"old_seed": seed,
				"new_seed": blocks[len(blocks)-1],
			}).Info("finished block process")
			seed = blocks[len(blocks)-1]

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
