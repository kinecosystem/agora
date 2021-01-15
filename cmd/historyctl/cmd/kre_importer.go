package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbiface"
	"github.com/kinecosystem/agora-common/retry"
	"github.com/kinecosystem/agora-common/retry/backoff"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/kinecosystem/agora/pkg/transaction/history"
	historyrw "github.com/kinecosystem/agora/pkg/transaction/history/dynamodb"
)

var (
	tableName string
)

func importToDynamo(_ *cobra.Command, args []string) error {
	db := dynamodb.New(awsConfig)
	reader := historyrw.New(db)

	f, err := os.Open(args[0])
	if err != nil {
		return err
	}
	defer f.Close()

	loadCh := make(chan string, workers)

	for i := 0; i < workers; i++ {
		id := i
		go csvLoaderBatch(id, db, reader, loadCh)
	}

	s := bufio.NewScanner(f)
	for s.Scan() {
		loadCh <- s.Text()
	}
	close(loadCh)
	log.WithError(err).Debug("Scan Complete")
	if err := s.Err(); err != nil {
		log.Fatal(err)
	}

	return nil
}

func csvLoaderBatch(id int, db dynamodbiface.ClientAPI, r history.Reader, loadCh <-chan string) {
	log := log.WithField("id", id)

	var writes []dynamodb.WriteRequest
	for b := range loadCh {
		item, err := loadItem(r, b)
		if err != nil {
			log.WithError(err).Fatal("failed to load item")
		}

		writes = append(writes, dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: item,
			},
		})

		if len(writes) == 25 {
			_, err := retry.Retry(
				func() error {
					resp, err := db.BatchWriteItemRequest(&dynamodb.BatchWriteItemInput{
						RequestItems: map[string][]dynamodb.WriteRequest{
							tableName: writes,
						},
					}).Send(context.Background())
					if err != nil {
						log.WithError(err).WithField("writes", writes).Fatal("failed to write batch")
						return err
					}
					if len(resp.UnprocessedItems) > 0 {
						log.WithError(err).Warnf("incomplete batch write (%d processed)", len(writes)-len(resp.UnprocessedItems[tableName]))
						writes = resp.UnprocessedItems[tableName]
						return errors.New("partial batch")
					}

					return nil
				},
				retry.Limit(10),
				retry.BackoffWithJitter(backoff.BinaryExponential(time.Second), 10*time.Second, 0.1),
			)
			if err != nil {
				log.WithError(err).Fatal("failed to submit batch")
			}

			writes = nil
		}
	}

	if len(writes) > 0 {
		_, err := retry.Retry(
			func() error {
				resp, err := db.BatchWriteItemRequest(&dynamodb.BatchWriteItemInput{
					RequestItems: map[string][]dynamodb.WriteRequest{
						tableName: writes,
					},
				}).Send(context.Background())
				if err != nil {
					log.WithError(err).Fatal("failed to write batch")
					return err
				}
				if len(resp.UnprocessedItems) > 0 {
					log.WithError(err).Warn("incomplete batch write")
					writes = resp.UnprocessedItems[tableName]
					return errors.New("partial batch")
				}

				return nil
			},
			retry.Limit(10),
			retry.BackoffWithJitter(backoff.BinaryExponential(time.Second), 10*time.Second, 0.1),
		)
		if err != nil {
			log.WithError(err).Fatal("failed to submit batch")
		}

		writes = nil
	}

	log.Debug("worker complete")
}

func loadItem(r history.Reader, b string) (map[string]dynamodb.AttributeValue, error) {
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(b), &m); err != nil {
		return nil, err
	}

	if blockStr, ok := m["block"]; ok {
		switch t := blockStr.(type) {
		case string:
			block, err := strconv.ParseUint(t, 10, 64)
			if err != nil {
				return nil, errors.Wrap(err, "invalid block")
			}
			m["block"] = block
		case float64:
			m["block"] = uint64(t)
		default:
			return nil, errors.Errorf("unexpected block type: %T", t)
		}
	}

	if rawOffset, ok := m["instruction_offset"]; ok {
		offset := int(rawOffset.(float64))
		m["tx_id"] = fmt.Sprintf("%s:%d", m["tx_id"], offset)
	}

	return dynamodbattribute.MarshalMap(m)
}
