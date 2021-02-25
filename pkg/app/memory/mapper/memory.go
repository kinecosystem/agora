package memory

import (
	"context"
	"sync"

	"github.com/kinecosystem/agora-common/kin"
	"github.com/pkg/errors"

	"github.com/kinecosystem/agora/pkg/app"
)

type mapper struct {
	sync.Mutex
	mappings map[string]uint16
}

// New returns an in-memory app.Mapper
func New() app.Mapper {
	return &mapper{
		mappings: make(map[string]uint16),
	}
}

func (m *mapper) reset() {
	m.Lock()
	m.mappings = make(map[string]uint16)
	m.Unlock()
}

// Add implements app.Mapper.Add
func (m *mapper) Add(ctx context.Context, appID string, appIndex uint16) error {
	if !kin.IsValidAppID(appID) {
		return errors.New("invalid app ID")
	}

	if appIndex == 0 {
		return errors.New("cannot create mapping for app index 0")
	}

	m.Lock()
	defer m.Unlock()

	if _, exists := m.mappings[appID]; exists {
		return app.ErrMappingExists
	}

	m.mappings[appID] = appIndex
	return nil
}

// GetAppIndex implements app.Mapper.GetAppIndex
func (m *mapper) GetAppIndex(ctx context.Context, appID string) (appIndex uint16, err error) {
	m.Lock()
	defer m.Unlock()

	appIndex, exists := m.mappings[appID]
	if !exists {
		return 0, app.ErrMappingNotFound
	}

	return appIndex, nil
}
