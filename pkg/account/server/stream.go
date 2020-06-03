package server

import (
	"errors"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stellar/go/xdr"
)

type eventData struct {
	e xdr.TransactionEnvelope
	m xdr.TransactionMeta
}

type eventStream struct {
	sync.Mutex

	log *logrus.Entry

	closed   bool
	streamCh chan eventData
}

func newEventStream(bufferSize int) *eventStream {
	return &eventStream{
		log:      logrus.StandardLogger().WithField("type", "account/server/stream"),
		streamCh: make(chan eventData, bufferSize),
	}
}

func (s *eventStream) notify(e xdr.TransactionEnvelope, m xdr.TransactionMeta, timeout time.Duration) error {
	s.Lock()

	if s.closed {
		s.Unlock()
		return errors.New("cannot notify closed stream")
	}

	select {
	case s.streamCh <- eventData{e: e, m: m}:
	case <-time.After(timeout):
		s.Unlock()
		s.close()
		return errors.New("timed out sending events to account event stream")
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
