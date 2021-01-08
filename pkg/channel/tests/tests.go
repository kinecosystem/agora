package tests

import (
	"testing"

	"github.com/kinecosystem/agora-common/kin/version"
	"github.com/kinecosystem/go/keypair"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/channel"
)

type PoolCtor func(maxChannels int, kinVersion version.KinVersion, kp *keypair.Full, channelSalt string) (pool channel.Pool, err error)

// RunTestsMax1 runs tests on a channel pool.
func RunTests(t *testing.T, poolCtor PoolCtor, teardown func()) {
	for _, tf := range []func(*testing.T, PoolCtor){TestGetChannel} {
		tf(t, poolCtor)
		teardown()
	}
}

func TestGetChannel(t *testing.T, poolCtor PoolCtor) {
	kp, err := keypair.Random()
	require.NoError(t, err)
	pool, err := poolCtor(1, version.KinVersion3, kp, "somesalt")
	require.Nil(t, err)

	c1, err := pool.GetChannel()
	require.NoError(t, err)
	require.NotNil(t, c1)
	require.NotNil(t, c1.KP)
	require.NotNil(t, c1.Lock)

	// We have a max of 1 channel wallet, so this should error since it is in use
	c2, err := pool.GetChannel()
	require.Error(t, err)
	require.Nil(t, c2)

	err = c1.Lock.Unlock()
	require.NoError(t, err)

	// Test idempotence
	err = c1.Lock.Unlock()
	require.NoError(t, err)

	// The lock has been released, so we should be able to get a channel
	c2, err = pool.GetChannel()
	require.NoError(t, err)
	require.NotNil(t, c2)
	require.NotNil(t, c2.KP)
	require.NotNil(t, c2.Lock)
}
