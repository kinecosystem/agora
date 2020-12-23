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
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion/stellar"
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
	Use:   "get",
	Short: "Get the latest ingestion pointer",
	RunE:  getCommit,
}

var setCommitCmd = &cobra.Command{
	Use:   "set <new>",
	Short: "Set the ingestion pointer, using either the sequence or slot value",
	Args:  cobra.ExactArgs(1),
	RunE:  setCommit,
}

func init() {
	rootCmd.AddCommand(commitCmd)
	commitCmd.PersistentFlags().IntVar(&kinVersion, "v", 4, "Kin Version")

	commitCmd.AddCommand(getCommitCmd)
	commitCmd.AddCommand(setCommitCmd)
}

func getCommit(*cobra.Command, []string) error {
	v := model.KinVersion(kinVersion)
	c := dynamocommitter.New(dynamodb.New(awsConfig))

	ptr, err := c.Latest(context.Background(), ingestion.GetHistoryIngestorName(v))
	if err != nil {
		return errors.Wrap(err, "failed to get latest pointer")
	}

	switch v {
	case model.KinVersion_KIN2, model.KinVersion_KIN3:
		seq, err := stellar.SequenceFromPointer(ptr)
		if err != nil {
			return errors.Wrap(err, "invalid stellar pointer")
		}
		fmt.Println("Sequence:", seq)
	case model.KinVersion_KIN4:
		slot, err := solana.SlotFromPointer(ptr)
		if err != nil {
			return errors.Wrap(err, "invalid solana pointer")
		}
		fmt.Println("Slot:", slot)
	}

	return nil
}

func setCommit(_ *cobra.Command, args []string) error {
	v := model.KinVersion(kinVersion)
	c := dynamocommitter.New(dynamodb.New(awsConfig)).(*dynamocommitter.Committer)
	val, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return errors.Wrap(err, "invalid argument")
	}

	var ptr ingestion.Pointer

	switch v {
	case model.KinVersion_KIN2, model.KinVersion_KIN3:
		ptr = stellar.PointerFromSequence(v, uint32(val))
	case model.KinVersion_KIN4:
		ptr = solana.PointerFromSlot(val)
	}

	return c.CommitUnsafe(context.Background(), ingestion.GetHistoryIngestorName(v), ptr)
}
