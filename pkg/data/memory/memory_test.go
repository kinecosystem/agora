package memory

import (
	"github.com/kinecosystem/agora-transaction-services/pkg/data/tests"
	"testing"
)

func TestStore(t *testing.T) {
	testStore := New()
	teardown := func() {
		testStore.(*memory).reset()
	}

	tests.RunTests(t, testStore, teardown)
}
