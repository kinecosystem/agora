package events

import (
	"context"

	"github.com/kinecosystem/agora/pkg/events/eventspb"
)

type Hook func(*eventspb.Event)

type Submitter interface {
	Submit(context.Context, *eventspb.Event) error
}
