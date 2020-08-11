package memory

import (
	"testing"

	"github.com/kinecosystem/agora/pkg/app/tests"
)

func TestMapper(t *testing.T) {
	testMapper := New()
	teardown := func() {
		testMapper.(*mapper).reset()
	}
	tests.RunMapperTests(t, testMapper, teardown)
}
