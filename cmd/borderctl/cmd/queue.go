package cmd

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"log"
	"os"
	"strings"

	"github.com/jackc/pgx/v4"
	"github.com/mr-tron/base58"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/stellar/go/strkey"

	migrationpb "github.com/kinecosystem/agora/pkg/migration/proto"
)

var (
	batchSize         int
	ignoreZeroBalance bool
)

var queueCmd = &cobra.Command{
	Use:   "queue",
	Short: "queue account(s) for migration",
	RunE:  queueRun,
}

func init() {
	rootCmd.AddCommand(queueCmd)
	queueCmd.PersistentFlags().IntVarP(&batchSize, "batch-size", "b", 70000, "batch size")
	queueCmd.PersistentFlags().BoolVar(&ignoreZeroBalance, "ignore-balance", false, "ignore zero balance while migrating.")
}

func queueRun(_ *cobra.Command, args []string) error {
	keys := make([]ed25519.PublicKey, 0, len(args))
	postgresURLs := make([]string, 0)
	files := make([]string, 0)

	for _, arg := range args {
		if strings.HasPrefix(arg, "file://") {
			files = append(files, arg)
			continue
		}

		if strings.HasPrefix(arg, "postgresql://") {
			if ignoreZeroBalance {
				return errors.New("refusing to migrate zero balances from postgres.")
			}

			postgresURLs = append(postgresURLs, arg)
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
		req := &migrationpb.QueueRequest{}
		for _, k := range keys {
			req.Items = append(req.Items, &migrationpb.QueueRequest_QueueItem{
				Key:               k,
				IgnoreZeroBalance: ignoreZeroBalance,
			})
		}

		// todo: could do better batching, but w/e
		_, err := client.Queue(context.Background(), &migrationpb.QueueRequest{
			Items: req.Items,
		})
		if err != nil {
			return err
		}
	}

	for _, url := range postgresURLs {
		log.Println("Processing:", url)
		if err := queuePostgres(url); err != nil {
			return err
		}
	}

	for _, f := range files {
		log.Println("Processing:", f)
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

	req := &migrationpb.QueueRequest{}

	s := bufio.NewScanner(f)
	for s.Scan() {
		k, err := strkey.Decode(strkey.VersionByteAccountID, s.Text())
		if err != nil {
			k, err = base58.Decode(s.Text())
			if err != nil {
				return errors.Wrap(err, "invalid key")
			}
		}

		req.Items = append(req.Items, &migrationpb.QueueRequest_QueueItem{
			Key:               k,
			IgnoreZeroBalance: ignoreZeroBalance,
		})

		if len(req.Items) == batchSize {
			_, err := client.Queue(context.Background(), req)
			if err != nil {
				return errors.Wrap(err, "failed to submit batch")
			}

			req = &migrationpb.QueueRequest{}
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

func queuePostgres(url string) error {
	conn, err := pgx.Connect(context.Background(), url)
	if err != nil {
		return errors.Wrapf(err, "failed to connect to %s", url)
	}
	defer conn.Close(context.Background())

	req := &migrationpb.QueueRequest{}
	rows, err := conn.Query(context.Background(), "SELECT accountid from accounts where balance > 0")
	if err != nil {
		return errors.Wrap(err, "failed to query accounts")
	}

	for rows.Next() {
		var account string
		if err := rows.Scan(&account); err != nil {
			return errors.Wrap(err, "failed to scan")
		}

		k, err := strkey.Decode(strkey.VersionByteAccountID, account)
		if err != nil {
			return errors.Wrap(err, "invalid key format")
		}

		req.Items = append(req.Items, &migrationpb.QueueRequest_QueueItem{
			Key:               k,
			IgnoreZeroBalance: false,
		})
		if len(req.Items) == batchSize {
			_, err := client.Queue(context.Background(), req)
			if err != nil {
				return errors.Wrap(err, "failed to submit batch")
			}

			req = &migrationpb.QueueRequest{}
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
