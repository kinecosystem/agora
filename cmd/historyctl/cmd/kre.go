package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/mr-tron/base58/base58"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

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

var processCmd = &cobra.Command{
	Use:   "process",
	Short: "Process KRE data from history",
	RunE:  export,
}

var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "Exports KRE data from a dynamodb table",
	RunE:  dump,
}

var importCmd = &cobra.Command{
	Use:   "import <file>",
	Short: "Import CSV into a dynamodb table",
	Args:  cobra.ExactArgs(1),
	RunE:  importToDynamo,
}

var compactCmd = &cobra.Command{
	Use:   "compact <source-dir> <out-dir>",
	Short: "Compact KRE data files",
	Args:  cobra.ExactArgs(2),
	RunE:  compact,
}

func init() {
	rootCmd.AddCommand(kreCmd)

	kreCmd.AddCommand(processCmd)
	processCmd.Flags().StringVar(&solanaEndpoint, "endpoint", "", "solana endpoint")
	processCmd.Flags().StringVarP(&mint, "mint", "m", "kinXdEcpDQeHPEuQnqmUgtYykqKGVFq6CeVX5iAHJq6", "token mint")
	processCmd.Flags().Uint64VarP(&startBlock, "start-block", "s", 55405004, "Start block to dump (inclusive)")
	processCmd.Flags().Uint64VarP(&endBlock, "end-block", "e", 0, "Start block to dump (exclusive)")
	processCmd.Flags().IntVarP(&partitionSize, "partition-size", "p", 20_000, "Size of partitions to divide block space into")
	processCmd.Flags().IntVarP(&workers, "workers", "w", 64, "Number of concurrent workers to process partitions")

	kreCmd.AddCommand(exportCmd)
	exportCmd.Flags().IntVarP(&workers, "workers", "w", 64, "Number of concurrent workers to process partitions")
	exportCmd.Flags().StringVarP(&tableName, "table", "t", "", "Table to dump from")
	exportCmd.Flags().Uint64VarP(&startBlock, "start-block", "s", 55405004, "Start block to dump (inclusive)")
	exportCmd.Flags().Uint64VarP(&endBlock, "end-block", "e", 0, "Start block to dump (exclusive)")

	kreCmd.AddCommand(importCmd)
	importCmd.Flags().IntVarP(&workers, "workers", "w", 8, "Number of concurrent workers")
	importCmd.Flags().StringVarP(&tableName, "table", "t", "", "Table to load into")

	kreCmd.AddCommand(compactCmd)
	compactCmd.Flags().Int64VarP(&compactionSize, "compaction-size", "s", 10737418240, "Target compaction file(s) size")

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

	if err := c.mem.Add(ctx, appID, appIndex); err != nil && err != app.ErrMappingExists {
		return appIndex, errors.Wrap(err, "failed to update local cache")
	}

	return appIndex, nil
}

func dump(*cobra.Command, []string) error {
	db := dynamodb.New(awsConfig)

	var wg sync.WaitGroup
	scanCh := make(chan string, workers)
	scanFunc := func(i int) {
		defer wg.Done()

		pager := dynamodb.NewScanPaginator(db.ScanRequest(&dynamodb.ScanInput{
			TableName:     &tableName,
			Segment:       aws.Int64(int64(i)),
			TotalSegments: aws.Int64(int64(workers)),
		}))

		for pager.Next(context.Background()) {
			for _, item := range pager.CurrentPage().Items {
				m := make(map[string]interface{})

				for k, v := range item {
					if v.N != nil {
						n, err := strconv.ParseUint(*v.N, 10, 64)
						if err != nil {
							log.WithError(err).Fatal("invalid number type")
						}
						m[k] = n
					} else if v.S != nil {
						m[k] = *v.S
					} else {
						log.Fatal("unexpected type")
					}
				}

				txField, ok := m["tx_id"]
				if !ok {
					log.Fatal("missing tx id")
				}
				m["tx_id"] = strings.Split(txField.(string), ":")[0]

				b, err := json.Marshal(m)
				if err != nil {
					log.WithError(err).Fatal("failed to marshal item")
				}

				scanCh <- string(b)
			}
		}
		if pager.Err() != nil {
			log.WithError(pager.Err()).Fatal("failed to scan")
		}
	}

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		id := i
		go scanFunc(id)
	}

	fName := fmt.Sprintf("%s-%d-%d.json", tableName, startBlock, endBlock)
	f, err := os.Create(fmt.Sprintf("%s.tmp", fName))
	if err != nil {
		return err
	}

	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		defer f.Close()

		for s := range scanCh {
			if _, err := f.WriteString(s + "\n"); err != nil {
				log.WithError(err).Fatal("failed to write")
			}
		}
	}()

	wg.Wait()
	close(scanCh)
	<-doneCh

	if err := os.Rename(fmt.Sprintf("%s.tmp", fName), fName); err != nil {
		return errors.Wrap(err, "failed to rename file")
	}

	return nil
}

func export(*cobra.Command, []string) error {
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
