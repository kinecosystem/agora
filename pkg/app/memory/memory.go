package memory

import (
	"context"
	"sync"

	"github.com/pkg/errors"

	"github.com/kinecosystem/agora-transaction-services-internal/pkg/app"
)

type store struct {
	sync.Mutex
	configs map[uint16]app.Config
}

// New returns an in-memory app.ConfigStore
func New() app.ConfigStore {
	return &store{
		configs: make(map[uint16]app.Config),
	}
}

func (s *store) reset() {
	s.Lock()
	s.configs = make(map[uint16]app.Config)
	s.Unlock()
}

// Add implements app.ConfigStore.Add
func (s *store) Add(ctx context.Context, appIndex uint16, config *app.Config) error {
	if config == nil {
		return errors.New("config is nil")
	}

	if len(config.AppName) == 0 {
		return errors.New("app name has length of 0")
	}

	s.Lock()
	defer s.Unlock()

	if _, exists := s.configs[appIndex]; exists {
		return app.ErrExists
	}

	s.configs[appIndex] = *config
	return nil
}

// Get implements app.ConfigStore.Get
func (s *store) Get(ctx context.Context, appIndex uint16) (*app.Config, error) {
	s.Lock()
	defer s.Unlock()

	config, exists := s.configs[appIndex]
	if ! exists {
		return nil, app.ErrNotFound
	}

	return &config, nil
}
