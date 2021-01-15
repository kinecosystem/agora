package cmd

import (
	"bufio"
	"encoding/json"
	"os"
	"strconv"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var dupeCheck = &cobra.Command{
	Use:   "dupe-check",
	Short: "check for duplicate entries",
	Args:  cobra.ExactArgs(1),
	RunE: func(_ *cobra.Command, args []string) error {
		f, err := os.Open(args[0])
		if err != nil {
			return err
		}
		defer f.Close()

		txMap := make(map[string]struct{})

		s := bufio.NewScanner(f)
		for s.Scan() {
			var m map[string]interface{}
			if err := json.Unmarshal([]byte(s.Text()), &m); err != nil {
				return err
			}

			txHash, ok := m["tx_id"]
			if !ok {
				return errors.Errorf("missing id: %s", s.Text())
			}
			txID := txHash.(string)
			if offset, ok := m["instruction_offset"]; ok {
				txID += strconv.FormatUint(uint64(offset.(float64)), 10)
			}

			if _, exists := txMap[txID]; exists {
				log.WithField("tx_id", txHash.(string)).Info("duplicate detected")
			}
			txMap[txID] = struct{}{}
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(dupeCheck)
}
