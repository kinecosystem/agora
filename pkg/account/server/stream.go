package server

import (
	"errors"
	"sync"
	"time"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v3"
)

type eventNotification struct {
	// events are the events that should be sent to the client
	events accountpb.Events

	// terminateStream indicates if the stream should be closed after these events get sent
	terminateStream bool
}

type eventStream struct {
	sync.Mutex
	closed   bool
	streamCh chan eventNotification
}

func newEventStream(bufferSize int) *eventStream {
	return &eventStream{
		streamCh: make(chan eventNotification, bufferSize),
	}
}

func (a *eventStream) notify(e eventNotification, timeout time.Duration) error {
	a.Lock()

	if a.closed {
		a.Unlock()
		return errors.New("cannot notify closed stream")
	}

	select {
	case a.streamCh <- e:
	case <-time.After(timeout):
		a.Unlock()
		a.close()
		return errors.New("timed out sending events to account stream")
	}

	a.Unlock()
	return nil
}

func (a *eventStream) close() {
	a.Lock()
	defer a.Unlock()

	if a.closed {
		return
	}

	a.closed = true
	close(a.streamCh)
}
