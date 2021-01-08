package memory

import (
	"testing"

	"github.com/kinecosystem/agora-common/kin/version"
	"github.com/kinecosystem/go/keypair"

	"github.com/kinecosystem/agora/pkg/channel"
	"github.com/kinecosystem/agora/pkg/channel/tests"
)

func TestPool(t *testing.T) {
	tests.RunTests(t, func(maxChannels int, kinVersion version.KinVersion, kp *keypair.Full, channelSalt string) (pool channel.Pool, err error) {
		return New(maxChannels, kinVersion, kp, "somesalt")
	}, func() {})
}
