package memory

import (
	"testing"

	"github.com/kinecosystem/agora/pkg/account/tests"
)

func TestMapper(t *testing.T) {
	m := New()
	tests.RunTests(t, m, m.(*mapper).reset)
}
