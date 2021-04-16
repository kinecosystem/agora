package memory

import (
	"context"

	"google.golang.org/protobuf/types/known/timestamppb"

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

	event.SubmissionTime = timestamppb.Now()
	if err := event.Validate(); err != nil {
		return err
	}

	for _, h := range p.hooks {
		h(event)
	}

	return nil
}
