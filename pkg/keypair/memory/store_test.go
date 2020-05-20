package memory

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/keypair/tests"
)

func TestInMemoryKeystore(t *testing.T) {
	keystore, err := newStore()
	require.NoError(t, err)
	tests.RunKeystoreTests(t, keystore, keystore.(*store).resetStore)
}
