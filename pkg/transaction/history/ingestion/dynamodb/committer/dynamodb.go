// Package dynamodb implements a dynamodb backed ingestion.Committer.
//
// The storage structure uses a key-value mapping for each chain ingestor to
// the latest commit. For example:
//
//     | ingestor | latest |
//     |----------|--------|
//     |   kin2   |   v    |
//     |   kin3   |   v    |
//     |   kin4   |   v    |
//
// The commit function uses an atomic compare-and-swap to ensure that the
// parent matches the stored latest, and that the new latest is 'older' than
// the previous latest.
package dynamodb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbiface"
	dynamodbutil "github.com/kinecosystem/agora-common/aws/dynamodb/util"
	"github.com/pkg/errors"

	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
)

const (
	tableName        = "tx-history-commits"
	ingestorKey      = "ingestor"
	commitCondition  = "attribute_not_exists(latest) or (latest = :parent and latest < :block)"
	commitExpression = "SET latest = :block"
)

var (
	tableNameStr        = aws.String(tableName)
	commitConditionStr  = aws.String(commitCondition)
	commitExpressionStr = aws.String(commitExpression)
)

type committer struct {
	client dynamodbiface.ClientAPI
}

func New(client dynamodbiface.ClientAPI) ingestion.Committer {
	return &committer{
		client: client,
	}
}

// Commit implements ingesiton.Committer.Commit.
func (c *committer) Commit(ctx context.Context, name string, parent, block ingestion.Pointer) error {
	if parent == nil {
		parent = []byte{0}
	}

	_, err := c.client.UpdateItemRequest(&dynamodb.UpdateItemInput{
		TableName:           tableNameStr,
		ConditionExpression: commitConditionStr,
		UpdateExpression:    commitExpressionStr,
		Key: map[string]dynamodb.AttributeValue{
			ingestorKey: {S: aws.String(name)},
		},
		ExpressionAttributeValues: map[string]dynamodb.AttributeValue{
			":parent": {B: parent},
			":block":  {B: block},
		},
	}).Send(ctx)
	if err != nil {
		if dynamodbutil.IsConditionalCheckFailed(err) {
			return ingestion.ErrInvalidCommit
		}

		return err
	}

	return nil
}

// Latest implements ingesiton.Committer.Latest.
func (c *committer) Latest(ctx context.Context, name string) (ingestion.Pointer, error) {
	resp, err := c.client.GetItemRequest(&dynamodb.GetItemInput{
		TableName: tableNameStr,
		Key: map[string]dynamodb.AttributeValue{
			ingestorKey: {S: aws.String(name)},
		},
	}).Send(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get latest commit")
	}

	ptr, ok := resp.Item["latest"]
	if ok {
		return ptr.B, nil
	}

	return nil, nil
}
