package cmd

import (
	"context"
	"crypto/ed25519"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/mr-tron/base58"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/stellar/go/strkey"

	gcpb "github.com/kinecosystem/agora/pkg/gc/proto"
)

var (
	batchSize         int
	ignoreZeroBalance bool
)

var queueCmd = &cobra.Command{
	Use:   "queue",
	Short: "queue account(s) for garbage collection",
	RunE:  queueRun,
}

func init() {
	rootCmd.AddCommand(queueCmd)
	queueCmd.PersistentFlags().IntVarP(&batchSize, "batch-size", "b", 70000, "batch size")
}

func queueRun(_ *cobra.Command, args []string) error {
	keys := make([]ed25519.PublicKey, 0, len(args))
	files := make([]string, 0)

	for _, arg := range args {
		if strings.HasPrefix(arg, "file://") {
			files = append(files, arg[len("file://"):])
			continue
		}

		k, err := strkey.Decode(strkey.VersionByteAccountID, arg)
		if err != nil {
			k, err = base58.Decode(arg)
			if err != nil {
				return errors.Wrap(err, "invalid key")
			}
		}
		keys = append(keys, k)
	}

	if len(keys) > 0 {
		req := &gcpb.QueueRequest{}
		for _, k := range keys {
			req.Items = append(req.Items, &gcpb.QueueRequest_QueueItem{
				Key:               k,
				IgnoreZeroBalance: ignoreZeroBalance,
			})
		}

		// todo: could do better batching, but w/e
		_, err := client.Queue(context.Background(), &gcpb.QueueRequest{
			Items: req.Items,
		})
		if err != nil {
			return err
		}
	}

	for _, f := range files {
		log.Println("Processing file:", f)
		if err := queueFile(f, ignoreZeroBalance); err != nil {
			return err
		}
	}

	return nil
}

func queueFile(path string, ignoreZeroBalance bool) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}

	req := &gcpb.QueueRequest{}
	csvr := csv.NewReader(f)

	var count int
	var fields []string
	for fields, err = csvr.Read(); err == nil; fields, err = csvr.Read() {
		count++
		accountField := fields[0]
		k, err := strkey.Decode(strkey.VersionByteAccountID, accountField)
		if err != nil {
			k, err = base58.Decode(accountField)
			if err != nil {
				return errors.Wrapf(err, "invalid key: %s", accountField)
			}
		}

		req.Items = append(req.Items, &gcpb.QueueRequest_QueueItem{
			Key:               k,
			IgnoreZeroBalance: ignoreZeroBalance,
		})

		if len(req.Items) == batchSize {
			_, err := client.Queue(context.Background(), req)
			if err != nil {
				return errors.Wrap(err, "failed to submit batch")
			}

			req = &gcpb.QueueRequest{}
		}

		if count%10000 == 0 {
			fmt.Println("Processed:", count)
		}
	}

	if len(req.Items) > 0 {
		_, err := client.Queue(context.Background(), req)
		if err != nil {
			return errors.Wrap(err, "failed to submit batch")
		}
	}

	return nil
}
