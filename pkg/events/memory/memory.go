package memory

import (
	"context"

	"github.com/golang/protobuf/ptypes"

	"github.com/kinecosystem/agora/pkg/events"
	"github.com/kinecosystem/agora/pkg/events/eventspb"
)

type PubSub struct {
	hooks []events.Hook
}

func New(hooks ...events.Hook) *PubSub {
	return &PubSub{
		hooks: hooks,
	}
}

func (p *PubSub) Submit(ctx context.Context, event *eventspb.Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	event.SubmissionTime = ptypes.TimestampNow()
	if err := event.Validate(); err != nil {
		return err
	}

	for _, h := range p.hooks {
		h(event)
	}

	return nil
}
