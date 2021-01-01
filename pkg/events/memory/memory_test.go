package memory

import (
	"testing"

	"github.com/kinecosystem/agora/pkg/events"
	"github.com/kinecosystem/agora/pkg/events/tests"
)

func TestMemory(t *testing.T) {
	ctor := func(hooks ...events.Hook) (events.Submitter, func()) {
		return New(hooks...), func() {}
	}

	tests.RunTests(t, ctor)
}
