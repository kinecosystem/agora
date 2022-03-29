package dynamodb

import (
	"bytes"
	"context"
	"math"
	"strconv"
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

const (
	// 10 thousand is roughly an hour's worth of blocks (9000, if no variance).
	// We don't want this to be _too_ large in case the number of transactions
	// is high, exceeding the partition size of 10 GB.
	//
	// We can tune this in the future (remembering to mark the block number the change
	// was applied at) if it's too small (or big), but for now this should balance both
	// fairly well.
	//
	// This size also means that we _should_ only be looking at 1-2 partitions for
	// most query cases (KRE queries, other history builders, etc).
	blockPartitionSize = 10_000
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

// GetTransactions implements history.Reader.GetTransactions.
//
// We implement the 'global' transaction store by partitioning transactions
// by block ranges, for two reasons:
//
//   1. Scan() does _not_ provide any ordering, so a simple KV approach will not work
//      for the desired use case of scanning history in order.
//   2. Query() allows us ordering, but putting every transaction into a mega partition
//      is very dangerous (max 10 GB per partition, and performance degradation).
//
// Therefore, our structure for global data is structured by placing transactions into
// block partitions (keyed on the start (inclusive) of the range), and sorted based on
// the ordering key of the transaction itself (which internally contains the block). This
// is a common pattern for time series DBs[1], but here we use block instead of time, as
// we don't always have time.
//
// To query, we simply find the block partition that would contain the 'romBlock. From there,
// we grab transactions in that partition until we have enough transactions (limit or maxBlock),
// or we've run out of transactions in the partition. In the latter case, we move onto the next
// partition, repeating the process.
//
// Note: this format does suffer somewhat heavily if there is a lot of sparse data. For example,
//       if there was a transaction produced at block X, and then another one at X + 10,000,000,
//       we'd have to scan 10,000 empty partitions. Techniques to mitigate this are somewhat
//       complicated (but doable), however given that we generally have a sufficient production rate,
//       and our queries should be bounded, this shouldn't be a major issue.
//
// [1] https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-time-series.html
func (db *db) GetTransactions(ctx context.Context, startKey, endKey []byte, limit int) ([]*model.Entry, error) {
	if limit <= 0 {
		limit = 100
	}

	// If the ordering key is in the stellar range, then it's before
	// all of our stored history (block 0). However, in theory we could
	// be scanning into solana history.
	startBlock, err := model.BlockFromOrderingKey(startKey)
	if err == model.ErrInvalidOrderingKeyVersion {
		startBlock = 0
	} else if err != nil {
		return nil, errors.Wrap(err, "invalid start key")
	}

	// If the end block is not in solana history, then there's nothing
	// to search for.
	endBlock, err := model.BlockFromOrderingKey(endKey)
	if err == model.ErrInvalidOrderingKeyVersion {
		return nil, nil
	} else if err != nil {
		return nil, errors.Wrap(err, "invalid end key")
	}

	if endBlock == 0 {
		return nil, errors.New("maxBlock must be non-zero")
	}
	if bytes.Compare(startKey, endKey) >= 0 {
		return nil, errors.New("startKey must be < endKey")
	}

	start := make([]byte, len(startKey))
	copy(start, startKey)
	end := make([]byte, len(endKey))
	copy(end, endKey)

	var entries []*model.Entry
	for blockStart := (startBlock / blockPartitionSize) * blockPartitionSize; len(entries) < limit && blockStart <= endBlock; blockStart += blockPartitionSize {
		pager := dynamodb.NewQueryPaginator(db.client.QueryRequest(&dynamodb.QueryInput{
			TableName:              txHistoryTableStr,
			KeyConditionExpression: getTransactionHistoryStr,
			ExpressionAttributeValues: map[string]dynamodb.AttributeValue{
				":block_start": {N: aws.String(strconv.FormatUint(blockStart, 10))},
				":start":       {B: start},
				":end":         {B: endKey},
			},
			Limit:            aws.Int64(int64(limit)),
			ScanIndexForward: aws.Bool(true),
		}))
		for pager.Next(ctx) {
			for _, item := range pager.CurrentPage().Items {
				e, err := getEntry(item)
				if err != nil {
					return nil, errors.Wrap(err, "invalid entry")
				}

				// Dynamo returns [start, end] when using between, so we must
				// filter out end ourselves.
				orderingKey, err := e.GetOrderingKey()
				if err != nil {
					return nil, errors.Wrap(err, "failed to get ordering key from stored entry")
				}
				if bytes.Compare(orderingKey, endKey) >= 0 {
					return entries, nil
				}

				entries = append(entries, e)

				// query limit applies per request; we also need to limit
				// the total results.
				if limit > 0 && len(entries) >= limit {
					return entries, nil
				}
			}
		}
		if pager.Err() != nil {
			return nil, errors.Wrap(pager.Err(), "failed to query transactions")
		}
	}

	return entries, nil
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
			":account": {S: aws.String(account)},
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

	txHash, err := entry.GetTxID()
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

	if se := entry.GetSolana(); se != nil && se.Confirmed && se.Slot == 0 {
		return errors.New("cannot write a confirmed entry with no slot")
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

		// match is an acceptable update, so we perform it.
		_, err = db.client.PutItemRequest(&dynamodb.PutItemInput{
			TableName: txTableStr,
			Item: map[string]dynamodb.AttributeValue{
				txHashKey: {B: txHash},
				entryAttr: {B: entryBytes},
			},
		}).Send(ctx)
		if err != nil {
			return err
		}
	} else if err != nil {
		return errors.Wrap(err, "failed to insert tx entry")
	}

	// We only want to commit rooted/committed solana entries to
	// history.
	//
	// The reason we don't do this for tx-by-hash is that in order
	// for GetTransaction() to yield the raw transaction for non-rooted
	// transactions, we must store the entry ourselves.
	//
	// This type of use case is not applicable for tx-history, where
	// callers are operating on rooted history only, and can tolerate
	// the delay.
	if se := entry.GetSolana(); se != nil && se.Confirmed {
		_, err := db.client.PutItemRequest(&dynamodb.PutItemInput{
			TableName: txHistoryTableStr,
			Item: map[string]dynamodb.AttributeValue{
				historyKey:     {N: aws.String(strconv.FormatUint((se.Slot/blockPartitionSize)*blockPartitionSize, 10))},
				historySortKey: {B: orderingKey},
				entryAttr:      {B: entryBytes},
			},
		}).Send(ctx)
		if err != nil {
			return errors.Wrap(err, "failed to insert tx history entry")
		}
	}

	// If there is no slot, it means we don't have any information on its
	// ordering key, and therefore it is pointless (and incorrect) to store
	// under accounts.
	//
	// When we get slot information (confirmed or in progress), the data should
	// be filled in.
	if se := entry.GetSolana(); se != nil && se.Slot == 0 {
		return nil
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
	// won't observe the (first successful) write if the two writes occurred close
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

	if previous.Version <= model.KinVersion_KIN3 {
		if proto.Equal(previous, entry) {
			return nil
		}

		return errors.Wrap(history.ErrInvalidUpdate, "double insert with different entries detected")
	}

	prevSol := previous.GetSolana()
	sol := entry.GetSolana()

	// The only time a slot is immutable is after the transaction has
	// reached max lockout. However, we don't have access to that information
	// at this layer. Therefore, the best protection we can offer for slot is
	// to ensure we don't completely reset it.
	//
	// If we are comparing two non-zero slots, we assume that the caller has
	// provided us with a more up-to-date slot.
	if prevSol.Slot > 0 && sol.Slot == 0 {
		return errors.Wrapf(history.ErrInvalidUpdate, "double insert with different entries detected (slot, old: %d, new: 0)", prevSol.Slot)
	}

	// A confirmed block cannot be unconfirmed
	if prevSol.Confirmed && !sol.Confirmed {
		return errors.Wrapf(history.ErrInvalidUpdate, "double insert with different entries detected (confirmation status, old: %v", prevSol.Confirmed)
	}

	if !bytes.Equal(prevSol.Transaction, sol.Transaction) {
		return errors.Wrap(history.ErrInvalidUpdate, "double insert with different entries detected (transaction)")
	}

	// In theory an error can occur after submission.
	// Therefore, we only check equality if there's an error already set.
	if len(prevSol.TransactionError) > 0 && len(sol.TransactionError) > 0 && !bytes.Equal(prevSol.TransactionError, sol.TransactionError) {
		return errors.Wrap(history.ErrInvalidUpdate, "double insert with different entries detected (transaction error)")
	}

	return nil
}
