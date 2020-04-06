package memory

import (
	"context"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	commonpb "github.com/kinecosystem/kin-api/genproto/common/v3"

	"github.com/kinecosystem/agora-transaction-services-internal/pkg/data"
)

type memory struct {
	sync.Mutex
	mappings map[string]*commonpb.AgoraData
}

// New returns an in-memory data.Store.
func New() data.Store {
	return &memory{
		mappings: make(map[string]*commonpb.AgoraData),
	}
}

func (m *memory) reset() {
	m.Lock()
	m.mappings = make(map[string]*commonpb.AgoraData)
	m.Unlock()
}

// Add implements data.Store.Add.
func (m *memory) Add(_ context.Context, d *commonpb.AgoraData) error {
	k := string(d.ForeignKey[:29])

	m.Lock()
	defer m.Unlock()

	if _, exists := m.mappings[k]; exists {
		return data.ErrCollision
	}

	m.mappings[k] = proto.Clone(d).(*commonpb.AgoraData)
	return nil
}

// Get implements data.Store.Get.
func (m *memory) Get(_ context.Context, prefixOrKey []byte) (*commonpb.AgoraData, error) {
	var k string
	switch len(prefixOrKey) {
	case 29:
		k = string(prefixOrKey)
	case 32:
		k = string(prefixOrKey[:29])
	default:
		return nil, errors.Errorf("invalid prefixOrKey len: %d", len(prefixOrKey))
	}

	m.Lock()
	defer m.Unlock()

	d, exists := m.mappings[k]
	if !exists {
		return nil, data.ErrNotFound
	}

	return proto.Clone(d).(*commonpb.AgoraData), nil
}
