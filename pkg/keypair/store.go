package keypair

import (
	"context"
	"errors"

	"github.com/kinecosystem/go/keypair"
)

var (
	ErrKeypairNotFound      = errors.New("keypair not found")
	ErrKeypairAlreadyExists = errors.New("keypair already exists")
)

type Keystore interface {
	// Put puts the provided full keypair into storage.
	//
	// ErrKeypairAlreadyExists is returned if a keypair with that id already exists.
	Put(ctx context.Context, id string, full *keypair.Full) error

	// Get fetches a keypair by id, if it exists in the store.
	//
	// ErrKeypairNotFound is returned if the keypair could not be found in the store.
	Get(ctx context.Context, id string) (*keypair.Full, error)
}
