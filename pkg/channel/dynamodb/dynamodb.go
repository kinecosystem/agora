package dynamodb

import (
	"math/rand"
	"strconv"
	"sync"
	"time"

	"cirello.io/dynamolock"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/kinecosystem/agora-common/retry"
	"github.com/kinecosystem/go/keypair"
	"github.com/pkg/errors"

	"github.com/kinecosystem/agora/pkg/channel"
)

const (
	maxAttempts = 100
	expiry      = 2 * time.Minute
)

var (
	tableName = "channel-locks"
)

type pool struct {
	client        *dynamolock.Client
	maxChannels   int
	rootAccountKP *keypair.Full
	channelSalt   string
}

func New(db dynamodbiface.DynamoDBAPI, maxChannels int, rootAccountKP *keypair.Full, channelSalt string) (channel.Pool, error) {
	client, err := dynamolock.New(
		db,
		tableName,
		dynamolock.WithLeaseDuration(expiry),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create dynamo lock client")
	}

	return &pool{
		client:        client,
		maxChannels:   maxChannels,
		rootAccountKP: rootAccountKP,
		channelSalt:   channelSalt,
	}, nil
}

func (p pool) GetChannel() (c *channel.Channel, err error) {
	var id int
	var dynamoLock *dynamolock.Lock

	_, err = retry.Retry(
		func() error {
			id = rand.Intn(p.maxChannels)
			dynamoLock, err = p.client.AcquireLock(strconv.Itoa(id), dynamolock.FailIfLocked())
			if err != nil {
				return err
			}

			return nil
		},
		retry.Limit(maxAttempts),
	)

	if err != nil {
		return nil, errors.Wrap(err, "failed to acquire locked channel")
	}

	kp, err := channel.GenerateChannelKeypair(p.rootAccountKP, id, p.channelSalt)
	if err != nil {
		return nil, errors.Wrap(err, "failed to generate keypair")
	}

	return &channel.Channel{
		KP:   kp,
		Lock: &lock{lock: dynamoLock},
	}, nil
}

type lock struct {
	mu sync.Mutex

	lock *dynamolock.Lock
}

// Unlock implements channel.Lock.Unlock
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
