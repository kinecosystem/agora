package tests

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/transaction/history"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion/memory"
	historymemory "github.com/kinecosystem/agora/pkg/transaction/history/memory"
)

type testEnv struct {
	writer    *historymemory.RW
	ingestor  *testIngestor
	testLock  *testLock
	committer ingestion.Committer
}

func setup(t *testing.T) (env testEnv) {
	env.writer = historymemory.New()
	env.ingestor = &testIngestor{queue: make(chan (<-chan ingestion.Result), 255)}
	env.testLock = &testLock{}
	env.committer = memory.New()

	return env
}

func TestRun_Cancellation(t *testing.T) {
	env := setup(t)

	ctx, cancel := context.WithCancel(context.Background())

	doneCh := make(chan struct{})
	go func() {
		err := ingestion.Run(ctx, env.testLock, env.committer, env.writer, env.ingestor)
		assert.Equal(t, context.Canceled, err)
		close(doneCh)
	}()

	// force a pause so the Ingest() function gets called correctly
	time.Sleep(100 * time.Millisecond)

	cancel()

	select {
	case <-doneCh:
	case <-time.After(10 * time.Second):
		t.Fatal("failed waiting for Run to stop")
	}
}

func TestRun_SingleRunner(t *testing.T) {
	env := setup(t)

	c := memory.New()

	for i := 0; i < cap(env.ingestor.queue); i++ {
		resultCh := make(chan ingestion.Result, 1)
		resultCh <- ingestion.Result{
			Parent: []byte{byte(i)},
			Block:  []byte{byte(i + 1)},
		}
		env.ingestor.queue <- resultCh
	}

	runner := func(c ingestion.Committer) {
		err := ingestion.Run(context.Background(), env.testLock, c, env.writer, env.ingestor)
		assert.Equal(t, context.Canceled, err)
	}

	doneCh := make(chan struct{})
	go func() {
		for len(env.ingestor.queue) > 0 {
			time.Sleep(10 * time.Millisecond)
		}
		close(doneCh)
	}()
	go runner(env.committer)
	go runner(c)

	<-doneCh

	p1, err := c.Latest(context.Background(), "test")
	require.NoError(t, err)
	p2, err := env.committer.Latest(context.Background(), "test")
	require.NoError(t, err)

	if p1 == nil {
		assert.NotNil(t, p2)
		assert.EqualValues(t, []byte{0xff}, p2)
	} else {
		assert.Nil(t, p2)
		assert.EqualValues(t, []byte{0xff}, p1)
	}
}

func TestRun_IngestionErrors(t *testing.T) {
	env := setup(t)

	// Set a one time error, ingestor should be tolerant
	env.ingestor.err = errors.New("test")
	for i := 0; i < cap(env.ingestor.queue); i++ {
		resultCh := make(chan ingestion.Result, 1)
		resultCh <- ingestion.Result{
			Parent: []byte{byte(i)},
			Block:  []byte{byte(i + 1)},
		}
		env.ingestor.queue <- resultCh
	}

	doneCh := make(chan struct{})
	go func() {
		for len(env.ingestor.queue) > 0 {
			time.Sleep(10 * time.Millisecond)
		}
		close(doneCh)
	}()
	go func() {
		err := ingestion.Run(context.Background(), env.testLock, env.committer, env.writer, env.ingestor)
		assert.Equal(t, context.Canceled, err)
	}()

	<-doneCh

	p, err := env.committer.Latest(context.Background(), "test")
	require.NoError(t, err)
	assert.EqualValues(t, []byte{0xff}, p)
}
func TestRun_CommitErrors(t *testing.T) {
	env := setup(t)

	// Set a one time error, Run should be tolerant
	c := &testCommitter{
		Committer: env.committer,
		err:       ingestion.ErrInvalidCommit,
	}

	for i := 0; i < cap(env.ingestor.queue); i++ {
		resultCh := make(chan ingestion.Result, 1)
		resultCh <- ingestion.Result{
			Parent: []byte{byte(i)},
			Block:  []byte{byte(i + 1)},
		}
		env.ingestor.queue <- resultCh
	}

	doneCh := make(chan struct{})
	go func() {
		for len(env.ingestor.queue) > 0 {
			time.Sleep(10 * time.Millisecond)
		}
		close(doneCh)
	}()
	go func() {
		err := ingestion.Run(context.Background(), env.testLock, c, env.writer, env.ingestor)
		assert.Equal(t, context.Canceled, err)
	}()

	<-doneCh

	p, err := env.committer.Latest(context.Background(), "test")
	require.NoError(t, err)
	assert.EqualValues(t, []byte{0xff}, p)
}

type testIngestor struct {
	err   error
	queue chan (<-chan ingestion.Result)
}

func (i *testIngestor) Name() string {
	return "test"
}

func (i *testIngestor) Ingest(ctx context.Context, w history.Writer, parent ingestion.Pointer) (ingestion.ResultQueue, error) {
	if i.err != nil {
		err := i.err
		i.err = nil

		return nil, err
	}

	return i.queue, nil
}

type testLock struct {
	mu sync.Mutex
}

func (l *testLock) Lock(ctx context.Context) error {
	l.mu.Lock()
	return nil
}

func (l *testLock) Unlock() error {
	l.mu.Unlock()
	return nil
}

type testCommitter struct {
	ingestion.Committer
	err error
}

func (t *testCommitter) Commit(ctx context.Context, name string, parent, block ingestion.Pointer) error {
	if t.err != nil {
		err := t.err
		t.err = nil
		return err
	}

	return t.Committer.Commit(ctx, name, parent, block)
}
