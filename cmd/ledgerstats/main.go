package main

import (
	"fmt"
	"os"

	"github.com/kinecosystem/agora-common/kin"
	"github.com/stellar/go/clients/horizonclient"
)

func run() error {
	client, err := kin.GetClientV2()
	if err != nil {
		return err
	}

	page, err := client.Ledgers(horizonclient.LedgerRequest{
		Cursor: "now",
		Order:  horizonclient.OrderDesc,
	})
	if err != nil {
		return err
	}

	for _, r := range page.Embedded.Records {
		counts := make(map[string]int)

		txns, err := client.Transactions(horizonclient.TransactionRequest{
			ForLedger: uint(r.Sequence),
		})
		if err != nil {
			return err
		}

		for _, t := range txns.Embedded.Records {
			if t.MemoType != "text" {
				counts["other"]++
				continue
			}

			counts[t.Memo[:5]]++
		}

		fmt.Printf("Ledger: %d\n---\n", r.Sequence)
		for k, v := range counts {
			fmt.Printf("%s:\t%d\n", k, v)
		}
		fmt.Println("")
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
