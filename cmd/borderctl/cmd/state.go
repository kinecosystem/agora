package cmd

import (
	"context"
	"fmt"
	"math"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbiface"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var clearStateCmd = &cobra.Command{
	Use:   "clear-state",
	Short: "clears the state for an account",
	Args:  cobra.MinimumNArgs(1),
	RunE:  clearStateRun,
}

func init() {
	rootCmd.AddCommand(clearStateCmd)
}

func clearStateRun(_ *cobra.Command, args []string) error {
	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		return errors.Wrap(err, "failed to init v2 aws sdk")
	}
	db := dynamodb.New(cfg)

	if len(args) == 1 && args[0] == "*" {
		return clearAll(db)
	}

	for _, account := range args {
		// todo: we could batch this, but this is mostly a debugging tool
		_, err := db.DeleteItemRequest(&dynamodb.DeleteItemInput{
			TableName: aws.String("migration-state"),
			Key: map[string]dynamodb.AttributeValue{
				"account": {S: aws.String(account)},
			},
		}).Send(context.Background())
		if err != nil {
			return err
		}
	}

	return nil
}

func clearAll(db dynamodbiface.ClientAPI) error {
	pager := dynamodb.NewScanPaginator(db.ScanRequest(&dynamodb.ScanInput{
		TableName:       aws.String("migration-state"),
		AttributesToGet: []string{"account"},
	}))

	var totalDeleted int

	for pager.Next(context.Background()) {
		var deletes []dynamodb.WriteRequest
		for _, item := range pager.CurrentPage().Items {
			deletes = append(deletes, dynamodb.WriteRequest{
				DeleteRequest: &dynamodb.DeleteRequest{
					Key: map[string]dynamodb.AttributeValue{
						"account": item["account"],
					},
				},
			})
		}

		totalDeleted += len(pager.CurrentPage().Items)
		fmt.Printf("Deleting %d entries (%d total)\n", len(pager.CurrentPage().Items), totalDeleted)
		for batchStart := 0; batchStart < len(deletes); batchStart += 25 {
			batchEnd := int(math.Min(float64(len(deletes)), float64(batchStart+25)))
			_, err := db.BatchWriteItemRequest(&dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]dynamodb.WriteRequest{
					"migration-state": deletes[batchStart:batchEnd],
				},
			}).Send(context.Background())
			if err != nil {
				return err
			}
		}
	}

	return pager.Err()
}
