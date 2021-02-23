package memory

import (
	"testing"

	"github.com/kinecosystem/agora/pkg/account/info/tests"
	"github.com/stretchr/testify/require"
)

func TestCache(t *testing.T) {
	testCache, err := NewCache(tests.TestTTL, tests.TestNegativeTTL, 5)
	require.NoError(t, err)

	teardown := func() {
		testCache.(*cache).reset()
	}
	tests.RunCacheTests(t, testCache, teardown)
}
