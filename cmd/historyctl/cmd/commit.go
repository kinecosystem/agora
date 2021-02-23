package cmd

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
	dynamocommitter "github.com/kinecosystem/agora/pkg/transaction/history/ingestion/dynamodb/committer"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion/solana"
	"github.com/kinecosystem/agora/pkg/transaction/history/kre"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

var (
	kinVersion int
)

var commitCmd = &cobra.Command{
	Use:   "commit",
	Short: "Interact with history commits",
}

var getCommitCmd = &cobra.Command{
	Use:   "get <type kre|history>",
	Short: "Get the latest ingestion pointer",
	Args:  cobra.ExactArgs(1),
	RunE:  getCommit,
}

var setCommitCmd = &cobra.Command{
	Use:   "set <type kre|history> <new>",
	Short: "Set the ingestion pointer, using either the sequence or slot value",
	Args:  cobra.ExactArgs(2),
	RunE:  setCommit,
}

func init() {
	rootCmd.AddCommand(commitCmd)
	commitCmd.AddCommand(getCommitCmd)
	commitCmd.AddCommand(setCommitCmd)
}

func getCommit(_ *cobra.Command, args []string) error {
	v := model.KinVersion(kinVersion)
	c := dynamocommitter.New(dynamodb.New(awsConfig))

	var ingestor string
	switch args[0] {
	case "history":
		ingestor = ingestion.GetHistoryIngestorName(v)
	case "kre":
		ingestor = kre.KREIngestorName
	default:
		return errors.Errorf("invalid commit type: %s", args[0])
	}

	ptr, err := c.Latest(context.Background(), ingestor)
	if err != nil {
		return errors.Wrap(err, "failed to get latest pointer")
	}

	slot, err := solana.SlotFromPointer(ptr)
	if err != nil {
		return errors.Wrap(err, "invalid solana pointer")
	}
	fmt.Println("Slot:", slot)

	return nil
}

func setCommit(_ *cobra.Command, args []string) error {
	v := model.KinVersion(kinVersion)
	c := dynamocommitter.New(dynamodb.New(awsConfig)).(*dynamocommitter.Committer)
	val, err := strconv.ParseUint(args[1], 10, 64)
	if err != nil {
		return errors.Wrap(err, "invalid argument")
	}

	var ingestor string
	switch args[0] {
	case "history":
		ingestor = ingestion.GetHistoryIngestorName(v)
	case "kre":
		ingestor = kre.KREIngestorName
	default:
		return errors.Errorf("invalid commit type: %s", args[0])
	}

	ptr := solana.PointerFromSlot(val)
	return c.CommitUnsafe(context.Background(), ingestor, ptr)
}
