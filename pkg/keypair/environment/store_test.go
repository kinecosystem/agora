package environment

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/keypair/tests"
)

func TestEnvironmentKeystore(t *testing.T) {
	keystore, err := newStore()
	require.NoError(t, err)
	tests.RunKeystoreTests(t, keystore, func() {
		os.Unsetenv(fmt.Sprintf(envVarFormat, defaultEnvVarPrefix, tests.TestKeypairID))
	})
}
