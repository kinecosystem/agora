package app

import (
	"context"

	"github.com/pkg/errors"
)

var (
	ErrExists   = errors.New("app index already exists")
	ErrNotFound = errors.New("app not found")
)

type ConfigStore interface {
	// Add adds an app's config to the store.
	//
	// Returns ErrExists if the specified app index already exists in the store.
	Add(ctx context.Context, appIndex uint16, config *Config) error

	// Get gets an app's config by app index
	//
	// Returns ErrNotFound if it could not be found.
	Get(ctx context.Context, appIndex uint16) (*Config, error)
}
