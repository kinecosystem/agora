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

var repairCmd = &cobra.Command{
	Use: "repair",
}

var timeRepairCmd = &cobra.Command{
	Use:   "time",
	Short: "Repairs missing timestamps over a range of history data",
	RunE:  timeRepair,
}

var driftRepairCmd = &cobra.Command{
	Use:   "drift",
	Short: "Repair the timestamp drift over a range of history data",
	RunE:  driftRepair,
}

var blockRepairCmd = &cobra.Command{
	Use:   "block",
	Short: "Repair the blocks over a range of history data",
	RunE:  blockRepair,
}

func init() {
	rootCmd.AddCommand(historyCmd)

	historyCmd.AddCommand(historyGetTxCmd)

	historyCmd.AddCommand(repairCmd)
	repairCmd.PersistentFlags().Uint64VarP(&startBlock, "start-block", "s", 55405004, "Start block to repair (inclusive)")
	repairCmd.PersistentFlags().Uint64VarP(&endBlock, "end-block", "e", 0, "Start block to repair (exclusive)")
	repairCmd.PersistentFlags().IntVarP(&partitionSize, "partition-size", "p", 20_000, "Size of partitions to divide block space into")
	repairCmd.PersistentFlags().IntVarP(&workers, "workers", "w", 64, "Number of concurrent workers to process partitions")
	repairCmd.PersistentFlags().BoolVar(&dryRun, "dry-run", true, "Whether or not to perform the repair")

	repairCmd.AddCommand(timeRepairCmd)
	timeRepairCmd.Flags().StringVar(&solanaEndpoint, "endpoint", "", "solana endpoint")

	repairCmd.AddCommand(driftRepairCmd)

	repairCmd.AddCommand(blockRepairCmd)
	blockRepairCmd.Flags().StringVar(&solanaEndpoint, "endpoint", "", "solana endpoint")
}

func getTxn(_ *cobra.Command, args []string) error {
	var marshaller jsonpb.Marshaler
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
		fmt.Println("Direct:")
		jStr, err := marshaller.MarshalToString(entry)
		if err != nil {
			return errors.Wrap(err, "failed to marshal stored entry")
		}
		fmt.Println("\t", jStr)

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

type Repairer interface {
	Repair(start, end uint64) error
}

func timeRepair(*cobra.Command, []string) error {
	rw := historyrw.New(dynamodb.New(awsConfig))
	repairer := newTimeRepairer(solana.New(solanaEndpoint), rw)
	return runRepair(repairer)
}

func driftRepair(*cobra.Command, []string) error {
	rw := historyrw.New(dynamodb.New(awsConfig))
	repairer := newDriftRepairer(rw)
	return runRepair(repairer)
}

func blockRepair(*cobra.Command, []string) error {
	rw := historyrw.New(dynamodb.New(awsConfig))
	repairer := newBlockRepairer(solana.New(solanaEndpoint), rw)
	return runRepair(repairer)
}

func runRepair(r Repairer) error {
	jobCh := make(chan job, workers)
	resultCh := make(chan result, workers)
	done := make(chan struct{})

	if startBlock < driftStartBlock {
		log.Infof("Snapping start to %d", driftStartBlock)
		startBlock = driftStartBlock
	}
	if endBlock == 0 || endBlock >= driftEndBlock {
		log.Infof("Snapping end to %d", driftEndBlock)
		endBlock = driftEndBlock
	}
	if startBlock >= endBlock {
		return errors.Errorf("invalid range: [%d, %d)", startBlock, endBlock)
	}

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go repairWorker(&wg, r, jobCh, resultCh)
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

func repairWorker(wg *sync.WaitGroup, repairer Repairer, jobCh <-chan job, resultCh chan<- result) {
	defer wg.Done()

	for j := range jobCh {
		err := repairer.Repair(j.start, j.end)
		resultCh <- result{
			job: j,
			err: err,
		}
	}
}
