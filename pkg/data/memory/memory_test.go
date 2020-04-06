package memory

import (
	"testing"

	"github.com/kinecosystem/agora-transaction-services-internal/pkg/data/tests"
)

func TestStore(t *testing.T) {
	testStore := New()
	teardown := func() {
		testStore.(*memory).reset()
	}

	tests.RunTests(t, testStore, teardown)
}
