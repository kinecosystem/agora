package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/events"
	"github.com/kinecosystem/agora/pkg/events/eventspb"
)

type SubmitterCtor func(hooks ...events.Hook) (submitter events.Submitter, teardown func())

func RunTests(t *testing.T, ctor SubmitterCtor) {
	for _, tf := range []func(t *testing.T, ctor SubmitterCtor){
		testRoundTrip,
		testCancellation,
		testValidation,
	} {
		tf(t, ctor)
	}
}

func testRoundTrip(t *testing.T, ctor SubmitterCtor) {
	t.Run("TestRoundTrip", func(t *testing.T) {
		hooks := make([]events.Hook, 5)
		called := make([][]*eventspb.Event, len(hooks))

		var wg sync.WaitGroup
		wg.Add(len(hooks))

		for i := 0; i < len(hooks); i++ {
			idx := i
			hooks[i] = func(e *eventspb.Event) {
				called[idx] = append(called[idx], proto.Clone(e).(*eventspb.Event))
				if len(called[idx]) == 5 {
					wg.Done()
				}
			}
		}

		s, teardown := ctor(hooks...)
		defer teardown()

		for i := 0; i < 5; i++ {
			e := &eventspb.Event{
				Kind: &eventspb.Event_TransactionEvent{
					TransactionEvent: &eventspb.TransactionEvent{
						Transaction:      []byte(fmt.Sprintf("hallo-%d", i)),
						TransactionError: []byte(fmt.Sprintf("salut-%d", i)),
					},
				},
			}
			assert.NoError(t, s.Submit(context.Background(), e))
			time.Sleep(10 * time.Millisecond)
		}

		wg.Wait()

		for _, events := range called {
			assert.Equal(t, 5, len(events))
			for i := 0; i < 5; i++ {
				if i > 0 {
					i0, err := ptypes.Timestamp(events[i-1].SubmissionTime)
					require.NoError(t, err)
					i1, err := ptypes.Timestamp(events[i].SubmissionTime)
					require.NoError(t, err)

					assert.True(t, i1.Sub(i0) >= 10*time.Millisecond)
				}
				assert.Equal(t, []byte(fmt.Sprintf("hallo-%d", i)), events[i].GetTransactionEvent().Transaction)
				assert.Equal(t, []byte(fmt.Sprintf("salut-%d", i)), events[i].GetTransactionEvent().TransactionError)
			}
		}
	})
}

func testCancellation(t *testing.T, ctor SubmitterCtor) {
	t.Run("TestCancellation", func(t *testing.T) {
		var called bool
		hook := func(_ *eventspb.Event) {
			called = true
		}

		s, teardown := ctor(hook)
		defer teardown()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := s.Submit(ctx, &eventspb.Event{
			Kind: &eventspb.Event_TransactionEvent{
				TransactionEvent: &eventspb.TransactionEvent{
					Transaction: []byte("hello"),
				},
			},
		})
		assert.Equal(t, context.Canceled, err)
		assert.False(t, called)
	})
}

func testValidation(t *testing.T, ctor SubmitterCtor) {
	t.Run("TestValidation", func(t *testing.T) {
		var called bool
		hook := func(_ *eventspb.Event) {
			called = true
		}

		s, teardown := ctor(hook)
		defer teardown()

		err := s.Submit(context.Background(), &eventspb.Event{
			Kind: &eventspb.Event_TransactionEvent{
				TransactionEvent: &eventspb.TransactionEvent{
					Transaction: make([]byte, 2000),
				},
			},
		})
		assert.NotNil(t, err)
		assert.False(t, called)
	})
}
