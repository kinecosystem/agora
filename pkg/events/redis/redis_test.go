package redis

import (
	"context"
	"os"
	"testing"

	"github.com/go-redis/redis/v7"
	redistest "github.com/kinecosystem/agora-common/redis/test"
	"github.com/ory/dockertest"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/events"
	"github.com/kinecosystem/agora/pkg/events/tests"
)

var redisConnString string

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

func TestRedis(t *testing.T) {
	ctor := func(hooks ...events.Hook) (events.Submitter, func()) {
		ps, err := New(
			redis.NewClient(&redis.Options{
				Addr: redisConnString,
			}),
			"test",
			hooks...,
		)
		require.NoError(t, err)

		return ps, func() { assert.NoError(t, ps.Close()) }
	}

	tests.RunTests(t, ctor)
}
