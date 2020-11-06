package rate

import (
	"context"
	"os"
	"testing"

	"github.com/go-redis/redis/v7"
	"github.com/go-redis/redis_rate/v8"
	redistest "github.com/kinecosystem/agora-common/redis/test"
	"github.com/ory/dockertest"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"
)

var (
	redisConnString string
)

func TestMain(m *testing.M) {
	log := logrus.StandardLogger()

	pool, err := dockertest.NewPool("")
	if err != nil {
		log.WithError(err).Error("Error creating docker pool")
		os.Exit(1)
	}

	var cleanUpFunc func()
	redisConnString, cleanUpFunc, err = redistest.StartRedis(context.Background(), pool)
	if err != nil {
		log.WithError(err).Error("Error starting redis connection")
		os.Exit(1)
	}

	code := m.Run()
	cleanUpFunc()
	os.Exit(code)
}

func TestNoLimiter(t *testing.T) {
	l := &NoLimiter{}
	for i := 0; i < 10000; i++ {
		allowed, err := l.Allow("")
		assert.NoError(t, err)
		assert.True(t, allowed)
	}
}

func TestLocalRateLimiter(t *testing.T) {
	l := NewLocalRateLimiter(rate.Limit(2))

	for i := 0; i < 2; i++ {
		allowed, err := l.Allow("a")
		assert.NoError(t, err)
		assert.True(t, allowed)
	}

	allowed, err := l.Allow("a")
	assert.NoError(t, err)
	assert.False(t, allowed)

	// Ensure key partitioning is valid
	for i := 0; i < 2; i++ {
		allowed, err := l.Allow("b")
		assert.NoError(t, err)
		assert.True(t, allowed)
	}

	allowed, err = l.Allow("b")
	assert.NoError(t, err)
	assert.False(t, allowed)
}

func TestRedisRateLimiter(t *testing.T) {
	ring := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{
			"server1": redisConnString,
		},
	})
	l := NewRedisRateLimiter(redis_rate.NewLimiter(ring), redis_rate.PerSecond(2))

	for i := 0; i < 2; i++ {
		allowed, err := l.Allow("a")
		assert.NoError(t, err)
		assert.True(t, allowed)
	}

	allowed, err := l.Allow("a")
	assert.NoError(t, err)
	assert.False(t, allowed)

	// Ensure key partitioning is valid
	for i := 0; i < 2; i++ {
		allowed, err := l.Allow("b")
		assert.NoError(t, err)
		assert.True(t, allowed)
	}

	allowed, err = l.Allow("b")
	assert.NoError(t, err)
	assert.False(t, allowed)
}
