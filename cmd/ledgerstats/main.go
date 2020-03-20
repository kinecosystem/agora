package main

import (
	"fmt"
	"os"

	"github.com/kinecosystem/agora-common/kin"
	"github.com/stellar/go/clients/horizonclient"
	hProtocol "github.com/stellar/go/protocols/horizon"
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
		opCount := make(map[string]int)
		txCount := make(map[string]int)

		var txnPage hProtocol.TransactionsPage
		txnPage, err = client.Transactions(horizonclient.TransactionRequest{
			ForLedger: uint(r.Sequence),
			Limit:     100,
		})
		if err != nil {
			return err
		}

		for len(txnPage.Embedded.Records) > 0 {
			for _, t := range txnPage.Embedded.Records {
				var k string
				if t.MemoType != "text" {
					k = "other"
				} else {
					k = t.Memo[:5]
				}

				opCount[k] += int(t.OperationCount)
				txCount[k]++
			}

			txnPage, err = client.NextTransactionsPage(txnPage)
			if err != nil {
				fmt.Println("Failed to get complete results")
				break
			}
		}

		fmt.Printf("Ledger (key, tx, op):%d\n---\n", r.Sequence)
		for k, v := range opCount {
			fmt.Printf("%s:\t%d\t%d\n", k, txCount[k], v)
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
