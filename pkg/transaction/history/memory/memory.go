package memory

import (
	"context"
	"sync"

	"github.com/golang/protobuf/proto"

	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

type Writer struct {
	sync.Mutex
	Writes []*model.Entry
}

func New() *Writer {
	return new(Writer)
}

// Write implements history.Writer.Write.
func (rw *Writer) Write(_ context.Context, e *model.Entry) error {
	rw.Lock()
	defer rw.Unlock()

	rw.Writes = append(rw.Writes, proto.Clone(e).(*model.Entry))

	return nil
}

// Reeset resets the recored writes.
func (rw *Writer) Reset() {
	rw.Lock()
	defer rw.Unlock()

	rw.Writes = nil
}
