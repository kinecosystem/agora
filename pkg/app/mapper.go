package app

import (
	"context"
	"errors"
)

var (
	ErrMappingExists   = errors.New("app ID mapping already exists")
	ErrMappingNotFound = errors.New("app ID mapping not found")
)

type Mapper interface {
	// Add adds a mapping between an app ID and an app index.
	//
	// Returns ErrMappingNotExists if a mapping with the specified app ID already exists in the store.
	Add(ctx context.Context, appID string, appIndex uint16) error

	// GetAppIndex gets the app index given an app ID.
	//
	// Returns ErrMappingNotFound if no mapping with the specified app ID was found.
	GetAppIndex(ctx context.Context, appID string) (appIndex uint16, err error)
}
