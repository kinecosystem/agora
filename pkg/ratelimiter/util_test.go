package ratelimiter

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/go-redis/redis_rate/v8"
	"github.com/ory/dockertest"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	redistest "github.com/kinecosystem/agora-common/redis/test"
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

func TestCanProceed(t *testing.T) {
	key := "test"
	limiter := redis_rate.NewLimiter(redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{
			"server1": redisConnString,
		},
	}))

	canProceed, err := CanProceed(limiter, key, 1)
	require.NoError(t, err)
	assert.True(t, canProceed)

	canProceed, err = CanProceed(limiter, key, 1)
	require.NoError(t, err)
	assert.False(t, canProceed)

	// wait for the limit to reset
	time.Sleep(1 * time.Second)

	canProceed, err = CanProceed(limiter, key, 1)
	require.NoError(t, err)
	assert.True(t, canProceed)
}
