package dynamodb

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"cirello.io/dynamolock"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/kinecosystem/agora-common/retry"
	"github.com/kinecosystem/agora/pkg/version"
	"github.com/kinecosystem/go/keypair"
	"github.com/pkg/errors"

	"github.com/kinecosystem/agora/pkg/channel"
)

const (
	maxAttempts = 100
	heartbeat   = 10 * time.Second
)

var (
	tableName = "channel-locks"
)

type pool struct {
	client        *dynamolock.Client
	kinVersion    version.KinVersion
	maxChannels   int
	rootAccountKP *keypair.Full
	channelSalt   string
}

func New(db dynamodbiface.DynamoDBAPI, maxChannels int, kinVersion version.KinVersion, rootAccountKP *keypair.Full, channelSalt string) (channel.Pool, error) {
	client, err := dynamolock.New(
		db,
		tableName,
		dynamolock.WithHeartbeatPeriod(heartbeat),
		dynamolock.WithLeaseDuration(3*heartbeat),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create dynamo lock client")
	}

	return &pool{
		client:        client,
		maxChannels:   maxChannels,
		kinVersion:    kinVersion,
		rootAccountKP: rootAccountKP,
		channelSalt:   channelSalt,
	}, nil
}

func (p pool) GetChannel() (c *channel.Channel, err error) {
	var i int
	var dynamoLock *dynamolock.Lock

	_, err = retry.Retry(
		func() error {
			i = rand.Intn(p.maxChannels)
			id := fmt.Sprintf("%d-%d", p.kinVersion, i)
			dynamoLock, err = p.client.AcquireLock(id, dynamolock.FailIfLocked())
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

	kp, err := channel.GenerateChannelKeypair(p.rootAccountKP, i, p.channelSalt)
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
