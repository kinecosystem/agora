package server

import (
	"errors"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v3"
)

type eventStream struct {
	sync.Mutex
	closed   bool
	streamCh chan accountpb.Events
}

func newEventStream(bufferSize int) *eventStream {
	return &eventStream{
		streamCh: make(chan accountpb.Events, bufferSize),
	}
}

func (a *eventStream) notify(e *accountpb.Events, timeout time.Duration) error {
	events := proto.Clone(e).(*accountpb.Events)

	a.Lock()

	if a.closed {
		a.Unlock()
		return errors.New("cannot notify closed stream")
	}

	select {
	case a.streamCh <- *events:
	case <-time.After(timeout):
		a.Unlock()
		a.close()
		return errors.New("timed out sending events to account stream")
	}

	a.Unlock()
	return nil
}

func (a *eventStream) close() {
	logrus.StandardLogger().Info("close called")
	a.Lock()
	defer a.Unlock()

	if a.closed {
		return
	}

	a.closed = true
	close(a.streamCh)
}
