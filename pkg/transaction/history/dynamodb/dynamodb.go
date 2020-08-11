package dynamodb

import (
	"context"
	"math"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbiface"
	"github.com/golang/protobuf/proto"
	dynamodbutil "github.com/kinecosystem/agora-common/aws/dynamodb/util"
	"github.com/kinecosystem/agora-common/retry"
	"github.com/kinecosystem/agora-common/retry/backoff"
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

// GetLatestForAccount implements history.Reader.GetLatestForAccount.
func (db *db) GetLatestForAccount(ctx context.Context, account string) (*model.Entry, error) {
	resp, err := db.client.QueryRequest(&dynamodb.QueryInput{
		TableName:              txByAccountTableStr,
		KeyConditionExpression: getAccountLatestQueryStr,
		ExpressionAttributeValues: map[string]dynamodb.AttributeValue{
			":account": dynamodb.AttributeValue{S: aws.String(account)},
		},
		Limit:            aws.Int64(1),
		ScanIndexForward: aws.Bool(false),
	}).Send(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get latest entry")
	}

	if len(resp.Items) == 0 {
		return nil, history.ErrNotFound
	}

	return getEntry(resp.Items[0])
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
		ConditionExpression: writeTxConditionExpressionStr,
		Item: map[string]dynamodb.AttributeValue{
			txHashKey: {B: txHash},
			entryAttr: {B: entryBytes},
		},
	}).Send(ctx)
	if dynamodbutil.IsConditionalCheckFailed(err) {
		if err := db.checkDoubleInsertMatch(ctx, txHash, entry); err != nil {
			return err
		}
	} else if err != nil {
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

func (db *db) checkDoubleInsertMatch(ctx context.Context, txHash []byte, entry *model.Entry) error {
	var item map[string]dynamodb.AttributeValue

	// At this point, we've detected a double insert due to a condition failure.
	//
	// However, since we need to read back the existing item to verify that the two
	// entries are identical in a separate request, there's the possibility that we
	// won't observe the (first successful) write if the two writes occured close
	// together in time.
	//
	// We use consistent reads to address this. However, consistent reads are more
	// far more prone to errors. While we expect the outer caller to be retrying,
	// there's often a lot of overhead getting to this point. Given that this should
	// resolve fairly quickly, we use a retry here to optimistically save some work.
	_, err := retry.Retry(
		func() error {
			resp, err := db.client.GetItemRequest(&dynamodb.GetItemInput{
				TableName:      txTableStr,
				ConsistentRead: aws.Bool(true),
				Key: map[string]dynamodb.AttributeValue{
					txHashKey: {B: txHash},
				},
			}).Send(ctx)
			if err != nil {
				return err
			}

			item = resp.Item
			return nil
		},

		// We use a somewhat aggressive strategy, so we can fall back to the outer
		// retry logic if this doesn't resolve quickly.
		retry.Limit(3),
		retry.Backoff(backoff.Constant(500*time.Millisecond), 500*time.Millisecond),
	)
	if err != nil {
		return errors.Wrap(err, "failed to check double insert match")
	}

	if len(item) == 0 {
		return errors.New("double insert detected, but existing entry not found")
	}

	previous, err := getEntry(item)
	if err != nil {
		return err
	}

	if proto.Equal(previous, entry) {
		return nil
	}

	return errors.New("double insert with different entries detected")
}
