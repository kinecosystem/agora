package memory

import (
	"fmt"
	"math/rand"
	"sync"

	"github.com/kinecosystem/agora-common/retry"
	"github.com/kinecosystem/agora/pkg/version"
	"github.com/kinecosystem/go/keypair"
	"github.com/pkg/errors"

	"github.com/kinecosystem/agora/pkg/channel"
)

const (
	maxAttempts = 100
)

type pool struct {
	mu       sync.Mutex
	channels map[string]*lock

	maxChannels   int
	kinVersion    version.KinVersion
	rootAccountKP *keypair.Full
	channelSalt   string
}

// New returns an in-memory implementation of channel.Pool
func New(maxChannels int, kinVersion version.KinVersion, rootAccountKP *keypair.Full, channelSalt string) (channel.Pool, error) {
	if maxChannels <= 0 {
		return nil, errors.New("maxChannels must be sent to an integer above 0")
	}
	return &pool{
		channels:      make(map[string]*lock),
		maxChannels:   maxChannels,
		kinVersion:    kinVersion,
		rootAccountKP: rootAccountKP,
		channelSalt:   channelSalt,
	}, nil
}

// GetChannel implements channel.Pool.GetChannel
func (p *pool) GetChannel() (c *channel.Channel, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var i int
	var id string

	_, err = retry.Retry(
		func() error {
			i = rand.Intn(p.maxChannels)
			id = fmt.Sprintf("%d-%d", p.kinVersion, i)

			l, ok := p.channels[id]
			if ok && l.locked {
				return errors.New("channel in use")
			}

			if l == nil {
				p.channels[id] = &lock{locked: true}
			} else {
				p.channels[id].locked = true
			}

			return nil
		},
		retry.Limit(maxAttempts),
	)
	if err != nil {
		return nil, err
	}

	kp, err := channel.GenerateChannelKeypair(p.rootAccountKP, i, p.channelSalt)
	if err != nil {
		return nil, err
	}

	return &channel.Channel{
		KP:   kp,
		Lock: p.channels[id],
	}, err
}

type lock struct {
	mu sync.Mutex

	locked bool
}

// Unlock implements channel.Lock.Unlock
func (l *lock) Unlock() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.locked = false
	return nil
}
