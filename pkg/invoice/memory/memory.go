package memory

import (
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
	entries map[string]entry
}

// New returns an in-memory invoice.Store.
func New() invoice.Store {
	return &memory{
		entries: make(map[string]entry),
	}
}

func (m *memory) reset() {
	m.Lock()
	m.entries = make(map[string]entry)
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

	if _, exists := m.entries[k]; exists {
		return invoice.ErrExists
	}

	m.entries[k] = entry{
		txHash:   txHash,
		contents: proto.Clone(inv).(*commonpb.Invoice),
	}
	return nil
}

// Get implements invoice.Store.Get.
func (m *memory) Get(_ context.Context, invoiceHash []byte) (*invoice.Record, error) {
	if len(invoiceHash) != 28 {
		return nil, errors.Errorf("invalid invoice hash len: %d", len(invoiceHash))
	}

	m.Lock()
	defer m.Unlock()

	entry, exists := m.entries[string(invoiceHash)]
	if !exists {
		return nil, invoice.ErrNotFound
	}

	return &invoice.Record{
		Invoice: proto.Clone(entry.contents).(*commonpb.Invoice),
		TxHash:  entry.txHash,
	}, nil
}
