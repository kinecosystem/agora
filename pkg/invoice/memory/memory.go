package memory

import (
	"bytes"
	"context"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	commonpb "github.com/kinecosystem/kin-api/genproto/common/v3"

	"github.com/kinecosystem/agora-transaction-services-internal/pkg/invoice"
)

type entry struct {
	txHash   []byte
	contents *commonpb.Invoice
}

type memory struct {
	sync.Mutex
	entries map[string][]entry
}

// New returns an in-memory invoice.Store.
func New() invoice.Store {
	return &memory{
		entries: make(map[string][]entry),
	}
}

func (m *memory) reset() {
	m.Lock()
	m.entries = make(map[string][]entry)
	m.Unlock()
}

// Add implements invoice.Store.Add.
func (m *memory) Add(_ context.Context, inv *commonpb.Invoice, txHash []byte) error {
	invoiceHash, err := invoice.GetHash(inv)
	if err != nil {
		return errors.Wrap(err, "failed to get invoice hash")
	}

	k := string(invoiceHash)
	m.Lock()
	defer m.Unlock()

	if entryList, exists := m.entries[k]; exists {
		for _, e := range entryList {
			if bytes.Equal(e.txHash, txHash) {
				return invoice.ErrExists
			}
		}
	}

	m.entries[k] = append(m.entries[k], entry{
		txHash:   txHash,
		contents: proto.Clone(inv).(*commonpb.Invoice),
	})
	return nil
}

// Get implements invoice.Store.Get.
func (m *memory) Get(_ context.Context, invoiceHash []byte, txHash []byte) (*commonpb.Invoice, error) {
	if len(invoiceHash) != 28 {
		return nil, errors.Errorf("invalid invoice hash len: %d", len(invoiceHash))
	}

	if len(txHash) != 32 {
		return nil, errors.Errorf("invalid transaction hash len: %d", len(txHash))
	}

	m.Lock()
	defer m.Unlock()

	entryList, exists := m.entries[string(invoiceHash)]
	if !exists {
		return nil, invoice.ErrNotFound
	}

	for _, e := range entryList {
		if bytes.Equal(e.txHash, txHash) {
			return proto.Clone(e.contents).(*commonpb.Invoice), nil
		}
	}
	return nil, invoice.ErrNotFound
}

// Exists implements invoice.Store.Exists
func (m *memory) Exists(_ context.Context, invoiceHash []byte) (bool, error) {
	entryList, exists := m.entries[string(invoiceHash)]
	return exists && len(entryList) > 0, nil
}