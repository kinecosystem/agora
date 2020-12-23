package cmd

import (
	"context"
	"math"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/mr-tron/base58/base58"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/kinecosystem/agora/pkg/app"
	dbmapper "github.com/kinecosystem/agora/pkg/app/dynamodb/mapper"
	memmapper "github.com/kinecosystem/agora/pkg/app/memory/mapper"
	historyreader "github.com/kinecosystem/agora/pkg/transaction/history/dynamodb"
	"github.com/kinecosystem/agora/pkg/transaction/history/processor"
)

var (
	solanaEndpoint string
	mint           string

	startBlock    uint64
	endBlock      uint64
	partitionSize int
	workers       int

	compactionSize int64
)

var kreCmd = &cobra.Command{
	Use:   "kre",
	Short: "Tools for interacting with KRE data",
}

var dumpCmd = &cobra.Command{
	Use:   "dump",
	Short: "Dumps KRE data from history",
	RunE:  dump,
}

var compactCmd = &cobra.Command{
	Use:   "compact <source-dir> <out-dir>",
	Short: "Compact KRE data files",
	Args:  cobra.ExactArgs(2),
	RunE:  compact,
}

func init() {
	rootCmd.AddCommand(kreCmd)

	kreCmd.AddCommand(dumpCmd)
	dumpCmd.Flags().StringVar(&solanaEndpoint, "endpoint", "", "solana endpoint")
	dumpCmd.Flags().StringVarP(&mint, "mint", "m", "kinXdEcpDQeHPEuQnqmUgtYykqKGVFq6CeVX5iAHJq6", "token mint")
	dumpCmd.Flags().Uint64VarP(&startBlock, "start-block", "s", 55405004, "Start block to dump (inclusive)")
	dumpCmd.Flags().Uint64VarP(&endBlock, "end-block", "e", 0, "Start block to dump (exclusive)")
	dumpCmd.Flags().IntVarP(&partitionSize, "partition-size", "p", 20_000, "Size of partitions to divide block space into")
	dumpCmd.Flags().IntVarP(&workers, "workers", "w", 64, "Number of concurrent workers to process partitions")

	kreCmd.AddCommand(compactCmd)
	compactCmd.Flags().Int64VarP(&compactionSize, "compaction-size", "s", 157286400, "Target compaction file(s) size")
}

type job struct {
	start uint64
	end   uint64
}
type result struct {
	job job
	err error
}

type cachedMapper struct {
	mem app.Mapper
	db  app.Mapper
}

func (c *cachedMapper) Add(ctx context.Context, appID string, appIndex uint16) error {
	return errors.New("cannot write to readonly mapper")
}

func (c *cachedMapper) GetAppIndex(ctx context.Context, appID string) (appIndex uint16, err error) {
	appIndex, err = c.mem.GetAppIndex(ctx, appID)
	if err == nil {
		return appIndex, nil
	} else if err != app.ErrMappingNotFound {
		return appIndex, err
	}

	appIndex, err = c.db.GetAppIndex(ctx, appID)
	if err != nil {
		return appIndex, err
	}

	if err := c.mem.Add(ctx, appID, appIndex); err != nil {
		return appIndex, errors.Wrap(err, "failed to update local cache")
	}

	return appIndex, nil
}

func dump(*cobra.Command, []string) error {
	mintPub, err := base58.Decode(mint)
	if err != nil {
		return errors.Wrap(err, "invalid mint")
	}

	reader := historyreader.New(dynamodb.New(awsConfig))
	mapper := &cachedMapper{
		mem: memmapper.New(),
		db:  dbmapper.New(dynamodb.New(awsConfig)),
	}

	sc := solana.New(solanaEndpoint)
	tc := token.NewClient(sc, mintPub)

	p := processor.NewProcessor(
		reader,
		nil, //unused
		nil, //unused
		"",
		tc,
		1024,
	)
	e := newExporter(p, mapper)

	jobCh := make(chan job, workers)
	resultCh := make(chan result, workers)
	done := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go dumpWorker(&wg, e, jobCh, resultCh)
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

func dumpWorker(wg *sync.WaitGroup, exporter *exporter, jobCh <-chan job, resultCh chan<- result) {
	defer wg.Done()

	for j := range jobCh {
		err := exporter.Export(j.start, j.end)
		resultCh <- result{
			job: j,
			err: err,
		}
	}
}
