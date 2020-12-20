package dynamodb

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/aws"
	dynamodbutil "github.com/kinecosystem/agora-common/aws/dynamodb/util"
	"github.com/pkg/errors"

	"github.com/kinecosystem/agora/pkg/transaction/dedupe"
)

const (
	table  = "tx-dedupe"
	idAttr = "id"

	conditionalUpdate = "attribute_not_exists(id)"
)

var (
	tableStr           = aws.String(table)
	conditionUpdateStr = aws.String(conditionalUpdate)
)

type db struct {
	client dynamodbiface.ClientAPI
	ttl    time.Duration
}

func New(client dynamodbiface.ClientAPI, ttl time.Duration) dedupe.Deduper {
	return &db{
		client: client,
		ttl:    ttl,
	}
}

func (d *db) Dedupe(ctx context.Context, id []byte, info *dedupe.Info) (prev *dedupe.Info, err error) {
	if len(id) == 0 {
		return nil, nil
	}

	if info == nil || len(info.Signature) == 0 {
		return nil, errors.Wrap(err, "cannot dedupe with without info")
	}

	item, err := getItem(id, info, d.ttl)
	if err != nil {
		return nil, err
	}

	_, err = d.client.PutItemRequest(&dynamodb.PutItemInput{
		TableName:           tableStr,
		ConditionExpression: conditionUpdateStr,
		Item:                item,
		ReturnValues:        dynamodb.ReturnValueAllOld,
	}).Send(ctx)
	if err != nil {
		if !dynamodbutil.IsConditionalCheckFailed(err) {
			return nil, errors.Wrap(err, "failed to update state")
		}

		resp, err := d.client.GetItemRequest(&dynamodb.GetItemInput{
			TableName: tableStr,
			Key: map[string]dynamodb.AttributeValue{
				idAttr: {B: id},
			},
		}).Send(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to load previous entry")
		}

		// It's possible that a delete occurred before we managed to read
		// the previous value. However, this loop can go on for a while, so
		// we just error here.
		if len(resp.Item) == 0 {
			return nil, errors.New("prev entry no longer exists")
		}

		prev, err := getInfo(resp.Item)
		if err != nil {
			return nil, err
		}
		return prev, nil
	}

	return nil, nil
}

func (d *db) Update(ctx context.Context, id []byte, info *dedupe.Info) error {
	if len(id) == 0 {
		return nil
	}

	item, err := getItem(id, info, d.ttl)
	if err != nil {
		return err
	}

	_, err = d.client.PutItemRequest(&dynamodb.PutItemInput{
		TableName: tableStr,
		Item:      item,
	}).Send(ctx)
	return err
}

func (d *db) Delete(ctx context.Context, id []byte) error {
	if len(id) == 0 {
		return nil
	}

	_, err := d.client.DeleteItemRequest(&dynamodb.DeleteItemInput{
		TableName: tableStr,
		Key: map[string]dynamodb.AttributeValue{
			idAttr: {B: id},
		},
	}).Send(ctx)
	return err
}
