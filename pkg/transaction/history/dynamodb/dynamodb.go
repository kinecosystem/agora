package dynamodb

import (
	"context"
	"math"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbiface"
	"github.com/golang/protobuf/proto"
	dynamodbutil "github.com/kinecosystem/agora-common/aws/dynamodb/util"
	"github.com/pkg/errors"

	"github.com/kinecosystem/agora/pkg/transaction/history"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

type db struct {
	client dynamodbiface.ClientAPI
}

func New(client dynamodbiface.ClientAPI) history.ReaderWriter {
	return &db{
		client: client,
	}
}

// GetTransaction implements history.Reader.GetTransaction.
func (db *db) GetTransaction(ctx context.Context, txHash []byte) (*model.Entry, error) {
	resp, err := db.client.GetItemRequest(&dynamodb.GetItemInput{
		TableName: txTableStr,
		Key: map[string]dynamodb.AttributeValue{
			txHashKey: {B: txHash},
		},
	}).Send(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get tx")
	}

	if len(resp.Item) == 0 {
		return nil, history.ErrNotFound
	}

	return getEntry(resp.Item)
}

// GetAccountTransactions implements history.Reader.GetGetAccountTransactions.
func (db *db) GetAccountTransactions(ctx context.Context, account string, opts *history.ReadOptions) ([]*model.Entry, error) {
	limit := opts.GetLimit()
	if limit <= 0 {
		limit = 100
	}

	var condition *string
	if opts.GetDescending() {
		condition = getAccountTransactionsDescQueryStr
	} else {
		condition = getAccountTransactionsAscQueryStr
	}

	pager := dynamodb.NewQueryPaginator(db.client.QueryRequest(&dynamodb.QueryInput{
		TableName:              txByAccountTableStr,
		KeyConditionExpression: condition,
		ExpressionAttributeValues: map[string]dynamodb.AttributeValue{
			":account": {S: aws.String(account)},
			":start":   {B: opts.GetStart()},
		},
		Limit:            aws.Int64(int64(limit)),
		ScanIndexForward: aws.Bool(!opts.GetDescending()),
	}))

	var entries []*model.Entry
	for pager.Next(ctx) {
		for _, item := range pager.CurrentPage().Items {
			e, err := getEntry(item)
			if err != nil {
				return nil, errors.Wrap(err, "invalid entry")
			}

			entries = append(entries, e)

			// query limit applies per request; we also need to limit
			// the total results.
			if len(entries) >= limit {
				return entries, nil
			}
		}
	}
	if pager.Err() != nil {
		return nil, errors.Wrap(pager.Err(), "failed to page account history")
	}

	return entries, nil
}

// Write implements history.Writer.Write.
func (db *db) Write(ctx context.Context, entry *model.Entry) error {
	if entry == nil {
		return errors.New("missing entry")
	}

	txHash, err := entry.GetTxHash()
	if err != nil {
		return errors.Wrap(err, "failed to get tx hash")
	}

	accounts, err := entry.GetAccounts()
	if err != nil {
		return errors.Wrap(err, "failed to get related accounts")
	}

	orderingKey, err := entry.GetOrderingKey()
	if err != nil {
		return errors.Wrap(err, "failed to get order key")
	}

	entryBytes, err := proto.Marshal(entry)
	if err != nil {
		return errors.Wrap(err, "failed to marshal entry")
	}

	_, err = db.client.PutItemRequest(&dynamodb.PutItemInput{
		TableName:           txTableStr,
		ConditionExpression: writeConditionExpressionStr,
		Item: map[string]dynamodb.AttributeValue{
			txHashKey: {B: txHash},
			entryAttr: {B: entryBytes},
		},
	}).Send(ctx)
	if err != nil && !dynamodbutil.IsConditionalCheckFailed(err) {
		return errors.Wrap(err, "failed to insert tx entry")
	}

	writes := make([]dynamodb.WriteRequest, len(accounts))
	for i := range accounts {
		writes[i] = dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]dynamodb.AttributeValue{
					accountKey:      {S: aws.String(accounts[i])},
					orderingSortKey: {B: orderingKey},
					entryAttr:       {B: entryBytes},
				},
			},
		}
	}

	for start := 0; start < len(writes); start += 25 {
		end := int(math.Min(float64(start+25), float64(len(writes))))

		_, err := db.client.BatchWriteItemRequest(&dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]dynamodb.WriteRequest{
				txByAccountTable: writes[start:end],
			},
		}).Send(ctx)
		if err != nil {
			return errors.Wrap(err, "failed to insert batch of account txns")
		}
	}

	return nil
}
