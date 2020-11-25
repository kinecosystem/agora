package memory

import (
	"testing"

	"github.com/kinecosystem/agora/pkg/migration/tests"
)

func TestStore(t *testing.T) {
	testStore := New()
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunStoreTests(t, testStore, teardown)
}
