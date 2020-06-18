package memory

import (
	"math/rand"
	"sync"

	"github.com/kinecosystem/agora-common/retry"
	"github.com/kinecosystem/go/keypair"
	"github.com/pkg/errors"

	"github.com/kinecosystem/agora/pkg/channel"
)

const (
	maxAttempts = 100
)

type pool struct {
	mu       sync.Mutex
	channels map[int]*lock

	maxChannels   int
	rootAccountKP *keypair.Full
	channelSalt   string
}

// New returns an in-memory implementation of channel.Pool
func New(maxChannels int, rootAccountKP *keypair.Full, channelSalt string) (channel.Pool, error) {
	if maxChannels <= 0 {
		return nil, errors.New("maxChannels must be sent to an integer above 0")
	}
	return &pool{
		channels:      make(map[int]*lock),
		maxChannels:   maxChannels,
		rootAccountKP: rootAccountKP,
		channelSalt:   channelSalt,
	}, nil
}

// GetChannel implements channel.Pool.GetChannel
func (p *pool) GetChannel() (c *channel.Channel, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var id int

	_, err = retry.Retry(
		func() error {
			id := rand.Intn(p.maxChannels)

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

	kp, err := channel.GenerateChannelKeypair(p.rootAccountKP, id, p.channelSalt)
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
