package keypair

import (
	"sync"

	"github.com/pkg/errors"
)

var (
	mapMutex          sync.RWMutex
	storeConstructors = make(map[string]StoreConstructor)
)

// A StoreConstructor creates a Keystore.
type StoreConstructor func() (Keystore, error)

// RegisterStoreConstructor registers a StoreConstructor of type storeType to be used to create a
// keystore.
func RegisterStoreConstructor(storeType string, constructor StoreConstructor) {
	mapMutex.Lock()
	storeConstructors[storeType] = constructor
	mapMutex.Unlock()
}

// CreateStore creates a Keystore of type storeType using the StoreConstructor of that type, if one
// has been registered. If no constructor has been registered with the specified type, an error is
// thrown
func CreateStore(storeType string) (Keystore, error) {
	mapMutex.RLock()
	constructor, ok := storeConstructors[storeType]
	mapMutex.RUnlock()
	if !ok {
		return nil, errors.Errorf("StoreConstructor with type %s not found", storeType)
	}
	return constructor()
}
