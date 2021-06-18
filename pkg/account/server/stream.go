package server

import (
	"errors"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v4"
)

type eventStream struct {
	sync.Mutex

	log *logrus.Entry

	closed   bool
	streamCh chan *accountpb.Event
}

func newEventStream(bufferSize int) *eventStream {
	return &eventStream{
		log:      logrus.StandardLogger().WithField("type", "account/stream"),
		streamCh: make(chan *accountpb.Event, bufferSize),
	}
}

func (s *eventStream) notify(event *accountpb.Event) error {
	event = proto.Clone(event).(*accountpb.Event)

	s.Lock()

	if s.closed {
		s.Unlock()
		return errors.New("cannot notify closed stream")
	}

	select {
	case s.streamCh <- event:
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
