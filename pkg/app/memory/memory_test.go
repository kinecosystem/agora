package memory

import (
	"testing"

	"github.com/kinecosystem/agora/pkg/app/tests"
)

func TestStore(t *testing.T) {
	testStore := New()
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunTests(t, testStore, teardown)
}
