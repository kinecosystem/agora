package cmd

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/golang/protobuf/jsonpb"
	"github.com/mr-tron/base58/base58"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/kinecosystem/agora-common/solana"
	historyrw "github.com/kinecosystem/agora/pkg/transaction/history/dynamodb"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

var (
	dryRun bool
)

var historyCmd = &cobra.Command{
	Use:   "history",
	Short: "History interaction and debugging",
}

var historyGetTxCmd = &cobra.Command{
	Use:   "get",
	Short: "Get a (set) of transaction(s) from history",
	Args:  cobra.MinimumNArgs(1),
	RunE:  getTxn,
}

var historyRepairCmd = &cobra.Command{
	Use:   "repair",
	Short: "Repair the timestamps over a range of history data",
	RunE:  repair,
}

func init() {
	rootCmd.AddCommand(historyCmd)

	historyCmd.AddCommand(historyGetTxCmd)

	historyCmd.AddCommand(historyRepairCmd)
	historyRepairCmd.Flags().StringVar(&solanaEndpoint, "endpoint", "", "solana endpoint")
	historyRepairCmd.Flags().Uint64VarP(&startBlock, "start-block", "s", 55405004, "Start block to dump (inclusive)")
	historyRepairCmd.Flags().Uint64VarP(&endBlock, "end-block", "e", 0, "Start block to dump (exclusive)")
	historyRepairCmd.Flags().IntVarP(&partitionSize, "partition-size", "p", 20_000, "Size of partitions to divide block space into")
	historyRepairCmd.Flags().IntVarP(&workers, "workers", "w", 64, "Number of concurrent workers to process partitions")
	historyRepairCmd.Flags().BoolVar(&dryRun, "dry-run", true, "Whether or not to perform the repair")
}

func getTxn(_ *cobra.Command, args []string) error {
	reader := historyrw.New(dynamodb.New(awsConfig))

	for i := range args {
		txID, err := base58.Decode(args[i])
		if err != nil {
			return errors.Wrapf(err, "invalid id at %d", i)
		}

		entry, err := reader.GetTransaction(context.Background(), txID)
		if err != nil {
			return errors.Wrap(err, "failed to get entry")
		}

		se := entry.GetSolana()
		if se == nil {
			log.Info("not a solana entry, continuing")
			continue
		}

		orderingKey, _ := entry.GetOrderingKey()
		transactions, err := reader.GetTransactions(context.Background(), orderingKey, append(orderingKey, 1), 1024)
		if err != nil {
			return errors.Wrap(err, "failed to get transactions for block")
		}

		var matching []*model.Entry
		for i, e := range transactions {
			entryID, _ := e.GetTxID()
			if bytes.Equal(entryID, txID) {
				matching = append(matching, transactions[i])
			}
		}

		fmt.Printf("%s:\n", args[i])
		var marshaller jsonpb.Marshaler
		for _, m := range matching {
			jStr, err := marshaller.MarshalToString(m)
			if err != nil {
				return errors.Wrap(err, "failed to marshal stored entry")
			}
			fmt.Println("\t", jStr)
		}
	}
	return nil
}

func repair(*cobra.Command, []string) error {
	rw := historyrw.New(dynamodb.New(awsConfig))
	repairer := newRepairer(solana.New(solanaEndpoint), rw)

	jobCh := make(chan job, workers)
	resultCh := make(chan result, workers)
	done := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go repairWorker(&wg, repairer, jobCh, resultCh)
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

func repairWorker(wg *sync.WaitGroup, repairer *repairer, jobCh <-chan job, resultCh chan<- result) {
	defer wg.Done()

	for j := range jobCh {
		err := repairer.Repair(j.start, j.end)
		resultCh <- result{
			job: j,
			err: err,
		}
	}
}
