package dynamodb

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"github.com/kinecosystem/agora-common/kin"
	"github.com/pkg/errors"
)

type mappingItem struct {
	AppID    string `dynamodbav:"app_id"`
	AppIndex uint16 `dynamodbav:"app_index"`
}

func toItem(appID string, appIndex uint16) (map[string]dynamodb.AttributeValue, error) {
	if !kin.IsValidAppID(appID) {
		return nil, errors.New("invalid app ID")
	}
	if appIndex == 0 {
		return nil, errors.New("cannot create mapping for app index 0")
	}

	return dynamodbattribute.MarshalMap(&mappingItem{
		AppID:    appID,
		AppIndex: appIndex,
	})
}

func fromItem(item map[string]dynamodb.AttributeValue) (uint16, error) {
	var mappingItem mappingItem
	if err := dynamodbattribute.UnmarshalMap(item, &mappingItem); err != nil {
		return 0, errors.Wrap(err, "failed to unmarshal mapping")
	}

	if mappingItem.AppIndex == 0 {
		return 0, errors.New("mapping has app index 0")
	}

	return mappingItem.AppIndex, nil
}
