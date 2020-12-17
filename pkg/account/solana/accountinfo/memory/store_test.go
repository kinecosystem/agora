package memory

import (
	"testing"

	"github.com/kinecosystem/agora/pkg/account/solana/accountinfo/tests"
)

func TestStore(t *testing.T) {
	testStore := NewStore()
	teardown := func() {
		testStore.(*store).reset()
	}
	tests.RunStateStoreTests(t, testStore, teardown)
}
