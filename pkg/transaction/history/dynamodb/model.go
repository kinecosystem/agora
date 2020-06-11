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

	txHashKey = "hash"

	accountKey      = "account"
	orderingSortKey = "ordering_key"

	entryAttr = "entry"

	writeConditionExpression        = "attribute_not_exists(account) and attribute_not_exists(ordering_key)"
	getAccountTransactionsAscQuery  = "account = :account and ordering_key >= :start"
	getAccountTransactionsDescQuery = "account = :account and ordering_key <= :start"
)

var (
	txTableStr                         = aws.String(txTable)
	txByAccountTableStr                = aws.String(txByAccountTable)
	writeConditionExpressionStr        = aws.String(writeConditionExpression)
	getAccountTransactionsAscQueryStr  = aws.String(getAccountTransactionsAscQuery)
	getAccountTransactionsDescQueryStr = aws.String(getAccountTransactionsDescQuery)
)

func getEntry(item map[string]dynamodb.AttributeValue) (*model.Entry, error) {
	rawEntry, ok := item["entry"]
	if !ok {
		return nil, errors.New("missing entry attribue")
	}

	entry := &model.Entry{}
	if err := proto.Unmarshal(rawEntry.B, entry); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal entry")
	}

	return entry, nil
}
