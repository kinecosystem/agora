package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	xrate "golang.org/x/time/rate"

	"github.com/kinecosystem/agora/pkg/rate"
)

func TestLimiter(t *testing.T) {
	l := NewLimiter(rate.NewLocalRateLimiter(xrate.Limit(5)))

	for i := 0; i < 5; i++ {
		allowed, err := l.Allow()
		assert.NoError(t, err)
		assert.True(t, allowed)
	}

	for i := 0; i < 5; i++ {
		allowed, err := l.Allow()
		assert.NoError(t, err)
		assert.False(t, allowed)
	}
}
