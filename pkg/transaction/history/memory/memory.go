package memory

import (
	"context"
	"sync"

	"github.com/golang/protobuf/proto"

	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

type RW struct {
	sync.Mutex
	Writes []*model.Entry
}

func New() *RW {
	return new(RW)
}

func (rw *RW) Write(_ context.Context, e *model.Entry) error {
	rw.Lock()
	defer rw.Unlock()

	rw.Writes = append(rw.Writes, proto.Clone(e).(*model.Entry))

	return nil
}

func (rw *RW) Reset() {
	rw.Lock()
	defer rw.Unlock()

	rw.Writes = nil
}
