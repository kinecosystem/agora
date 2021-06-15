package dynamodb

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"

	"github.com/kinecosystem/agora/pkg/migration"
)

const (
	stateTableKin2        = "migration-state-kin2"
	stateTableKin3        = "migration-state"
	stateTableHashKey     = "account"
	statePutExpression    = "attribute_not_exists(account) or (account = :account and #status = :prev_status and signature = :prev_signature)"
	stateUpdateExpression = "account = :account and #status = :prev_status and signature = :prev_signature"
)

var (
	stateTableKin2Str        = aws.String(stateTableKin2)
	stateTableKin3Str        = aws.String(stateTableKin3)
	stateTableHashKeyStr     = aws.String(stateTableHashKey)
	statePutExpressionStr    = aws.String(statePutExpression)
	stateUpdateExpressionStr = aws.String(stateUpdateExpression)
)

type stateItem struct {
	Account      string    `dynamodbav:"account"`
	Status       int       `dynamodbav:"status"`
	Signature    []byte    `dynamodbav:"signature"`
	LastModified time.Time `dynamodbav:"last_modified"`
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
