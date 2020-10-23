package memory

import (
	"context"
	"errors"
	"sync"

	"github.com/golang/protobuf/proto"

	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"

	"github.com/kinecosystem/agora/pkg/invoice"
)

type memory struct {
	sync.Mutex
	entries map[string]*commonpb.InvoiceList
}

// New returns an in-memory invoice.Store.
func New() invoice.Store {
	return &memory{
		entries: make(map[string]*commonpb.InvoiceList),
	}
}

func (m *memory) reset() {
	m.Lock()
	m.entries = make(map[string]*commonpb.InvoiceList)
	m.Unlock()
}

// Add implements invoice.Store.Add.
func (m *memory) Put(_ context.Context, txHash []byte, il *commonpb.InvoiceList) error {
	if len(txHash) != 32 && len(txHash) != 64 {
		return errors.New("txHash not 32 or 64 bytes")
	}

	k := string(txHash)

	m.Lock()
	defer m.Unlock()

	if _, exists := m.entries[k]; exists {
		return invoice.ErrExists
	}

	m.entries[k] = proto.Clone(il).(*commonpb.InvoiceList)
	return nil
}

// Get implements invoice.Store.Get.
func (m *memory) Get(_ context.Context, txHash []byte) (*commonpb.InvoiceList, error) {
	if len(txHash) != 32 && len(txHash) != 64 {
		return nil, errors.New("txHash not 32 or 64 bytes")
	}

	m.Lock()
	defer m.Unlock()

	entry, exists := m.entries[string(txHash)]
	if !exists {
		return nil, invoice.ErrNotFound
	}

	return proto.Clone(entry).(*commonpb.InvoiceList), nil
}
