package solana

import (
	"errors"
	"sync"

	"github.com/kinecosystem/agora-common/solana"
	"github.com/sirupsen/logrus"
)

type eventStream struct {
	sync.Mutex

	log *logrus.Entry

	closed   bool
	streamCh chan solana.BlockTransaction
}

func newEventStream(bufferSize int) *eventStream {
	return &eventStream{
		log:      logrus.StandardLogger().WithField("type", "account/solana/stream"),
		streamCh: make(chan solana.BlockTransaction, bufferSize),
	}
}

func (s *eventStream) notify(txn solana.BlockTransaction) error {
	s.Lock()

	if s.closed {
		s.Unlock()
		return errors.New("cannot notify closed stream")
	}

	select {
	case s.streamCh <- txn:
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
