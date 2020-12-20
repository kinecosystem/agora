package memory

import (
	"testing"

	"github.com/kinecosystem/agora/pkg/transaction/dedupe/tests"
)

func TestMemory(t *testing.T) {
	d := New()
	tests.RunTests(t, d, d.(*deduper).reset)
}
