package keypair

import (
	"context"
	"testing"

	"github.com/kinecosystem/go/keypair"
	"github.com/stretchr/testify/require"
)

func TestCreateStore(t *testing.T) {
	store, err := CreateStore("test")
	require.Error(t, err)
	require.Nil(t, store)

	RegisterStoreConstructor("test", newStore)

	store, err = CreateStore("test")
	require.NoError(t, err)
	require.NotNil(t, store)
}

type testStore struct {
}

func newStore() (Keystore, error) {
	return &testStore{}, nil
}

func (t testStore) Put(ctx context.Context, id string, full *keypair.Full) error {
	return nil
}

func (t testStore) Get(ctx context.Context, id string) (*keypair.Full, error) {
	return nil, nil
}
