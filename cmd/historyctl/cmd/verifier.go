package cmd

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/mr-tron/base58/base58"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/kinecosystem/agora/pkg/transaction/history"
	historyreader "github.com/kinecosystem/agora/pkg/transaction/history/dynamodb"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

var verifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "Verify integrity of history",
	RunE:  verifyHistory,
}

func init() {
	historyCmd.AddCommand(verifyCmd)

	verifyCmd.Flags().Uint64VarP(&startBlock, "start-block", "s", 55405004, "Start block to verify (inclusive)")
	verifyCmd.Flags().Uint64VarP(&endBlock, "end-block", "e", 0, "Start block to verify (exclusive)")
	verifyCmd.Flags().IntVarP(&partitionSize, "partition-size", "p", 20_000, "Size of partitions to divide block space into")
	verifyCmd.Flags().IntVarP(&workers, "workers", "w", 64, "Number of concurrent workers to process partitions")
}

func verifyHistory(*cobra.Command, []string) error {
	reader := historyreader.New(dynamodb.New(awsConfig))

	jobCh := make(chan job, workers)
	resultCh := make(chan result, workers)
	done := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()

			for j := range jobCh {
				err := verifyRange(reader, j.start, j.end)
				resultCh <- result{
					job: j,
					err: err,
				}
			}
		}()
	}

	go func() {
		defer close(done)
		for r := range resultCh {
			log := log.WithFields(log.Fields{
				"start": r.job.start,
				"end":   r.job.end,
			})
			if r.err == nil {
				log.Info("Finished successfully")
			} else {
				log.WithError(r.err).Warn("Failed")
			}
		}
	}()

	for i := startBlock; i < endBlock; i += uint64(partitionSize) {
		jobCh <- job{
			start: i,
			end:   uint64(math.Min(float64(endBlock), float64(i+uint64(partitionSize)))),
		}
	}
	close(jobCh)
	wg.Wait()
	close(resultCh)
	<-done

	return nil
}

func verifyRange(reader history.Reader, start, end uint64) error {
	last := model.OrderingKeyFromBlock(start)
	max := model.OrderingKeyFromBlock(end)

	for bytes.Compare(last, max) < 0 {
		entries, err := reader.GetTransactions(context.Background(), last, max, 1024)
		if err != nil {
			return errors.Wrapf(err, "failed to get transactions over [%d, %d)", start, end)
		}
		fmt.Println("got entries:", len(entries))

		for _, e := range entries {
			txID, err := e.GetTxID()
			if err != nil {
				return errors.Wrap(err, "failed to get entry id")
			}

			orderingKey, err := e.GetOrderingKey()
			if err != nil {
				return errors.Wrap(err, "failed to get entry ordering key")
			}

			last = append(orderingKey, 0)

			bt := e.GetSolana().BlockTime.AsTime()
			if bt.IsZero() || bt.Unix() == 0 {
				log.WithFields(log.Fields{
					"block": e.GetSolana().Slot,
					"tx_id": base58.Encode(txID),
					"time":  bt,
				}).Warn("invalid entry")
			}
		}

		if len(entries) == 0 {
			fmt.Println("no entries")
			break
		}
	}
	return nil
}
