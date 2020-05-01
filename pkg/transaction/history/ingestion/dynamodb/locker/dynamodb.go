package dynamodb

import (
	"context"
	"sync"
	"time"

	"cirello.io/dynamolock"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"

	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
)

var (
	tableName = "tx-history-locks"
)

type lock struct {
	client  *dynamolock.Client
	key     string
	refresh time.Duration

	mu   sync.Mutex
	lock *dynamolock.Lock
}

func New(db dynamodbiface.DynamoDBAPI, lockKey string, heartbeat time.Duration) (ingestion.DistributedLock, error) {
	client, err := dynamolock.New(
		db,
		tableName,
		dynamolock.WithLeaseDuration(3*heartbeat),
		dynamolock.WithHeartbeatPeriod(heartbeat),
	)
	if err != nil {
		return nil, err
	}

	return &lock{
		client:  client,
		key:     lockKey,
		refresh: heartbeat,
	}, nil
}

// Lock implements ingestion.DistributedLock.Lock.
func (l *lock) Lock(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// note: IsExpired handles nil-receivers.
	if !l.lock.IsExpired() {
		return nil
	}

	lock, err := l.client.AcquireLock(l.key, dynamolock.WithRefreshPeriod(l.refresh))
	if err != nil {
		return err
	}

	l.lock = lock
	return nil
}

// Lock implements ingestion.DistributedLock.Unlock.
func (l *lock) Unlock() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	err := l.lock.Close()
	switch err {
	case dynamolock.ErrLockAlreadyReleased, dynamolock.ErrCannotReleaseNullLock:
		return nil
	default:
		return err
	}
}
