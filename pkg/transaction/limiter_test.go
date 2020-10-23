package transaction

import (
	"testing"

	"github.com/stretchr/testify/assert"
	xrate "golang.org/x/time/rate"

	"github.com/kinecosystem/agora/pkg/rate"
)

func TestLimiter(t *testing.T) {
	l := NewLimiter(func(r int) rate.Limiter {
		return rate.NewLocalRateLimiter(xrate.Limit(r))
	}, 10, 5)

	for i := 0; i < 5; i++ {
		allowed, err := l.Allow(1)
		assert.NoError(t, err)
		assert.True(t, allowed)
	}

	allowed, err := l.Allow(1)
	assert.NoError(t, err)
	assert.False(t, allowed)

	for i := 0; i < 5; i++ {
		allowed, err := l.Allow(2)
		assert.NoError(t, err)
		assert.True(t, allowed)
	}

	allowed, err = l.Allow(2)
	assert.NoError(t, err)
	assert.False(t, allowed)
}
