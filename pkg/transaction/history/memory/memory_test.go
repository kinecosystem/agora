package memory

import (
	"testing"

	"github.com/kinecosystem/agora/pkg/transaction/history/tests"
)

func TestStore(t *testing.T) {
	rw := New()
	teardown := func() {
		rw.Reset()
	}

	tests.RunTests(t, rw, teardown)
}
