package dynamodb

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"

	"github.com/kinecosystem/agora/pkg/migration"
)

const (
	stateTable            = "migration-state"
	stateTableHashKey     = "account"
	statePutExpression    = "attribute_not_exists(account) or (account = :account and #status = :prev_status and signature = :prev_signature)"
	stateUpdateExpression = "account = :account and #status = :prev_status and signature = :prev_signature"

	requestTable        = "migration-request"
	requestTableHashKey = "account"
	requestUpdateExpr   = "set #count = if_not_exists(#count, :start) + :inc"
)

var (
	stateTableStr            = aws.String(stateTable)
	stateTableHashKeyStr     = aws.String(stateTableHashKey)
	statePutExpressionStr    = aws.String(statePutExpression)
	stateUpdateExpressionStr = aws.String(stateUpdateExpression)

	requestTableStr        = aws.String(requestTable)
	requestTableHashKeyStr = aws.String(requestTableHashKey)
	requestUpdateExprStr   = aws.String(requestUpdateExpr)
)

type stateItem struct {
	Account      string    `dynamodbav:"account"`
	Status       int       `dynamodbav:"status"`
	Signature    []byte    `dynamodbav:"signature"`
	LastModified time.Time `dynamodbav:"last_modified"`
}

type countItem struct {
	Account []byte `dynamodbav:"account"`
	Count   int    `dynamodbav:"count"`
}

func getState(item map[string]dynamodb.AttributeValue) (state migration.State, err error) {
	var mapped stateItem
	if err := dynamodbattribute.UnmarshalMap(item, &mapped); err != nil {
		return state, err
	}

	state.Status = migration.Status(mapped.Status)
	copy(state.Signature[:], mapped.Signature)
	state.LastModified = mapped.LastModified

	return state, nil
}

func getItem(account string, state migration.State) (map[string]dynamodb.AttributeValue, error) {
	stateItem := stateItem{
		Account:      account,
		Status:       int(state.Status),
		Signature:    state.Signature[:],
		LastModified: state.LastModified,
	}

	return dynamodbattribute.MarshalMap(&stateItem)
}

func getCount(item map[string]dynamodb.AttributeValue) (count int, err error) {
	var mapped countItem
	if err := dynamodbattribute.UnmarshalMap(item, &mapped); err != nil {
		return 0, err
	}

	return mapped.Count, nil
}
