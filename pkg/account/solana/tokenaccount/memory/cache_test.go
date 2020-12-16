package memory

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/account/solana/tokenaccount/tests"
)

func TestCache(t *testing.T) {
	testCache, err := New(tests.TestTTL, 5)
	require.NoError(t, err)

	teardown := func() {
		testCache.(*cache).reset()
	}
	tests.RunTests(t, testCache, teardown)
}
