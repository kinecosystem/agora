package memory

import (
	"testing"

	"github.com/kinecosystem/agora/pkg/channel"
	"github.com/kinecosystem/agora/pkg/channel/tests"
	"github.com/kinecosystem/go/keypair"
)

func TestPool(t *testing.T) {
	tests.RunTests(t, func(maxChannels int, kp *keypair.Full, channelSalt string) (pool channel.Pool, err error) {
		return New(maxChannels, kp, "somesalt")
	}, func() {})
}
