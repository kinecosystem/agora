package dynamodb

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

const (
	txTable          = "tx-by-hash"
	txByAccountTable = "tx-by-account"
	txHistoryTable   = "tx-history"

	txHashKey       = "tx_hash"
	accountKey      = "account"
	orderingSortKey = "ordering_key"
	historyKey      = "block_start"
	historySortKey  = "ordering_key"

	entryAttr = "entry"

	writeTxConditionExpression      = "attribute_not_exists(tx_hash)"
	getAccountLatestQuery           = "account = :account"
	getAccountTransactionsAscQuery  = "account = :account and ordering_key >= :start"
	getAccountTransactionsDescQuery = "account = :account and ordering_key <= :start"
	getTransactionHistoryQuery      = "block_start = :block_start and ordering_key between :start and :end"
)

var (
	txTableStr                         = aws.String(txTable)
	txByAccountTableStr                = aws.String(txByAccountTable)
	txHistoryTableStr                  = aws.String(txHistoryTable)
	writeTxConditionExpressionStr      = aws.String(writeTxConditionExpression)
	getAccountLatestQueryStr           = aws.String(getAccountLatestQuery)
	getAccountTransactionsAscQueryStr  = aws.String(getAccountTransactionsAscQuery)
	getAccountTransactionsDescQueryStr = aws.String(getAccountTransactionsDescQuery)
	getTransactionHistoryStr           = aws.String(getTransactionHistoryQuery)
)

func getEntry(item map[string]dynamodb.AttributeValue) (*model.Entry, error) {
	rawEntry, ok := item["entry"]
	if !ok {
		return nil, errors.New("missing entry attribute")
	}

	entry := &model.Entry{}
	if err := proto.Unmarshal(rawEntry.B, entry); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal entry")
	}

	return entry, nil
}
