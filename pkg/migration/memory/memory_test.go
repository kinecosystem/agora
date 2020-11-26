package memory

import (
	"testing"

	"github.com/kinecosystem/agora/pkg/migration/tests"
)

func TestStore(t *testing.T) {
	testStore := New()
	teardown := func() {
		testStore.(*Store).Reset()
	}
	tests.RunStoreTests(t, testStore, teardown)
}
