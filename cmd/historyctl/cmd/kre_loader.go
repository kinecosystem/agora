package cmd

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/mr-tron/base58/base58"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"google.golang.org/api/option"

	dbmapper "github.com/kinecosystem/agora/pkg/app/dynamodb/mapper"
	memmapper "github.com/kinecosystem/agora/pkg/app/memory/mapper"
	historyreader "github.com/kinecosystem/agora/pkg/transaction/history/dynamodb"
	"github.com/kinecosystem/agora/pkg/transaction/history/kre"
	bqsubmitter "github.com/kinecosystem/agora/pkg/transaction/history/kre/bigquery"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
	"github.com/kinecosystem/agora/pkg/transaction/history/processor"
)

var (
	gcloudCredentialsFile string
	gcloudProjectID       string
	creationsTable        string
	paymentsTable         string
	limit                 int
)

var loaderCmd = &cobra.Command{
	Use:   "debug-loader",
	Short: "Command line version of the KRE loader for debugging",
	RunE:  runDebugLoader,
}

func init() {
	kreCmd.AddCommand(loaderCmd)

	loaderCmd.Flags().StringVar(&gcloudCredentialsFile, "credentials", "", "Google Cloud credentials file")
	loaderCmd.Flags().StringVar(&gcloudProjectID, "project", "", "Google Cloud project id")
	loaderCmd.Flags().StringVarP(&creationsTable, "creations-table", "c", "", "Creations table")
	loaderCmd.Flags().StringVarP(&paymentsTable, "payments-table", "p", "", "Payments table")
	loaderCmd.Flags().Uint64VarP(&startBlock, "start-block", "s", 0, "Start block")
	loaderCmd.Flags().Uint64VarP(&endBlock, "end-block", "e", 0, "End block")
	loaderCmd.Flags().IntVarP(&limit, "limit", "l", 100, "Limit")
	loaderCmd.Flags().StringVar(&solanaEndpoint, "endpoint", "", "solana endpoint")
	loaderCmd.Flags().StringVarP(&mint, "mint", "m", "kinXdEcpDQeHPEuQnqmUgtYykqKGVFq6CeVX5iAHJq6", "token mint")
}

func runDebugLoader(*cobra.Command, []string) error {
	mintPub, err := base58.Decode(mint)
	if err != nil {
		return errors.Wrap(err, "invalid mint")
	}

	bqClient, err := bigquery.NewClient(
		context.Background(),
		gcloudProjectID,
		option.WithCredentialsFile(gcloudCredentialsFile),
	)
	if err != nil {
		return errors.Wrap(err, "failed to intiialize bigquery client")
	}

	tc := token.NewClient(solana.New(solanaEndpoint), mintPub)
	reader := historyreader.New(dynamodb.New(awsConfig))
	mapper := &cachedMapper{
		mem: memmapper.New(),
		db:  dbmapper.New(dynamodb.New(awsConfig)),
	}
	loader := kre.NewLoader(
		bqsubmitter.New(bqClient, creationsTable),
		bqsubmitter.New(bqClient, paymentsTable),
		mapper,
	)
	p := processor.NewProcessor(
		reader,
		nil,
		nil,
		"",
		tc,
		1024,
		loader.LoadData,
	)

	sc, err := p.ProcessRange(
		context.Background(),
		model.OrderingKeyFromBlock(startBlock),
		model.OrderingKeyFromBlock(endBlock),
		limit,
	)
	if err != nil {
		return errors.Wrap(err, "failed to process range")
	}

	return loader.LoadData(sc)
}
