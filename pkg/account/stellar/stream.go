package server

import (
	"errors"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/kinecosystem/agora/pkg/transaction/stellar"
)

type eventStream struct {
	sync.Mutex

	log *logrus.Entry

	closed   bool
	streamCh chan stellar.XDRData
}

func newEventStream(bufferSize int) *eventStream {
	return &eventStream{
		log:      logrus.StandardLogger().WithField("type", "account/server/stream"),
		streamCh: make(chan stellar.XDRData, bufferSize),
	}
}

func (s *eventStream) notify(xdrData stellar.XDRData) error {
	s.Lock()

	if s.closed {
		s.Unlock()
		return errors.New("cannot notify closed stream")
	}

	select {
	case s.streamCh <- xdrData:
	default:
		s.Unlock()
		s.close()
		return errors.New("account event stream channel full")
	}

	s.Unlock()
	return nil
}

func (s *eventStream) close() {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return
	}

	s.closed = true
	close(s.streamCh)
}
