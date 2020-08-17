package ingestion

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/kinecosystem/agora-common/retry"
	"github.com/kinecosystem/agora-common/retry/backoff"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/kinecosystem/agora/pkg/transaction/history"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

var (
	// ErrInvalidCommit indicates that the result could not be committed,
	// either due to the block being older than the latest committed block,
	// or the parent of the block does not match the latest committed block.
	//
	// Both cases should be resolvable by restarting the Ingestor from the
	// latest committed block.
	ErrInvalidCommit = errors.New("invalid commit")
)

// Pointer is an opaque value that points to a 'block' or 'ledger'
// for a given blockchain. The value should be lexicographically comparable
// to entries produced by the respective blockchain's model.Entry's ordering
// keys.
//
// For example, all transactions produced in blocks 0 -> N should be
// lexicographically less than the pointer to block N+1.
type Pointer []byte

// Result is the result of processing a block.
type Result struct {
	// Err is a non-retriable error that occurred when processing
	// the block.
	Err error

	// Parent points to the parent block that was processed.
	Parent Pointer

	// Block points to the block that was processed.
	Block Pointer
}

// ResultQueue behaves as a Queue<Future<Result>>, where the
// order of the queue is that of the order of blocks being produced.
//
// This structure allows for async processing of blocks, while ensuring
// that the commit pointer is advanced sequentially.
type ResultQueue <-chan <-chan Result

// Ingestor ingests blocks from a blockchain, writing them to a history.Writer.
type Ingestor interface {
	// Name is the name of the ingestor that will be used as the key when
	// committing blocks. As a result, it should be some unique identifier
	// referencing the blockchain being processed.
	Name() string

	// Ingest ingests blocks starting at the immediate block after the parent pointer.
	Ingest(ctx context.Context, w history.Writer, parent Pointer) (ResultQueue, error)
}

// DistributedLock is a distributed lock used for coordinating which node should
// be actively ingesting. While the Commit() ensures that no bad commits can be
// made (by the invariants described on the Committer interface), the DistributedLock
// helps avoid double processing of blocks, and churn from ErrInvalidCommit.
type DistributedLock interface {
	// Lock blocks until the lock is acquired, the context is cancelled, or an
	// unexpected error occured. In the latter two cases, an error is returned.
	Lock(ctx context.Context) error

	// Unlock releases the lock, if acquired. Unlock() is idempotent.
	Unlock() error
}

// Committer marks processed blocks as committed, with the following assumptions:
//
//   1. The block must be successfully written to history via a history.Writer.
//   2. The block must be committed in the same order as the block chain.
//
// Implementations should enforce (2) by ensuring that for each commit C[i], C[i].parent
// is the same as C[i-1].block (should a previous commit exist), and that C[i].block
// is greater than C[i].parent (and therefore greater than C[i-1].block).
type Committer interface {
	// Commit commits the specified block as committed, if 'parent' was the
	// value of the previously committed block.
	//
	// If no previous commit exists, the commit is considered valid.
	Commit(ctx context.Context, ingestor string, parent, block Pointer) error

	// Latest returns the latest committed block pointer.
	//
	// Nil is returned with a nil error if no previous commit exists.
	Latest(ctx context.Context, ingestor string) (Pointer, error)
}

// GetHistoryIngestorName returns the history ingestor name for the
// specified version.
func GetHistoryIngestorName(version model.KinVersion) string {
	return fmt.Sprintf("history_%s", version.String())
}

// GetEventsIngestorName returns the events ingestor name for the
// specified version.
func GetEventsIngestorName(version model.KinVersion) string {
	return fmt.Sprintf("events_%s", version.String())
}

// Run runs an ingestion flow in a blocking fashion. Run only returns
// if the context is cancelled.
//
// The ingestion will only occur when the lock has been acquired. The
// specified lock can be scoped to the individual ingestor, or for a set
// of ingestors.
func Run(ctx context.Context, l DistributedLock, c Committer, w history.Writer, i Ingestor) error {
	log := logrus.StandardLogger().WithFields(logrus.Fields{
		"type":     "transaction/history/ingestion",
		"method":   "Run",
		"ingestor": i.Name(),
	})

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		_, err := retry.Retry(
			func() error {
				return l.Lock(ctx)
			},
			retry.NonRetriableErrors(context.Canceled),
			retry.BackoffWithJitter(backoff.Constant(5*time.Second), 5*time.Second, 0.1),
		)
		if err != nil {
			return err
		}

		// If we continually fail to ingest, we give up the lock and try again
		// in case there's something specific to our process that's causing an issue,
		// such as a bad network connection, or invalid/expired permissions.
		_, err = retry.Retry(
			func() error {
				latest, err := c.Latest(ctx, i.Name())
				if err != nil {
					return errors.Wrapf(err, "failed to get latest commit for '%s'", i.Name())
				}

				// We create a specific queue context that 'defines' the lifetime of the Queue.
				//
				// When an error occurs, we want to restart the ingestion process. This creates
				// a new queue, and so we must ensure that we close the previous one in order to
				// prevent a memory leak. By tying each queue's lifetime to a context, we can
				// properly cleanup the underlying resources.
				queueCtx, cancel := context.WithCancel(ctx)
				defer cancel()

				log.WithField("parent", hex.EncodeToString(latest)).Debug("Starting ingestor")
				queue, err := i.Ingest(queueCtx, w, latest)
				if err != nil {
					return errors.Wrapf(err, "failed to start ingestor '%s'", i.Name())
				}

				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case resultCh, ok := <-queue:
						if !ok {
							// The queue will only ever be closed if there was a block processing err (in which case
							// we would have returned due to result.Err), or the context was cancelled, in which case
							// we'd want to cease processing.
							return nil
						}

						r := <-resultCh
						if r.Err != nil {
							return errors.Wrap(r.Err, "failed to ingest")
						}

						log.WithFields(logrus.Fields{
							"parent": hex.EncodeToString(r.Parent),
							"block":  hex.EncodeToString(r.Block),
						}).Trace("Committing block")

						if err := c.Commit(ctx, i.Name(), r.Parent, r.Block); err != nil {
							// If we get an ErrInvalidCommit, the two likely causes are:
							//   1. We've lost our lock status, and another process has been processing.
							//   2. Another node incorrectly believes it has the lock status.
							//   3. A process manually (or without checking the lock) committed a block.
							//
							// There's not much to be done about (3), but (1) and (2) are the same issue,
							// just from a different perspective. To alleviate them, we'll bail out and
							// try to acquire the lock again.
							if err == ErrInvalidCommit {
								return ErrInvalidCommit
							}

							return errors.Wrapf(err, "failed to commit block (%x, %x)", r.Parent, r.Block)
						}
					}
				}
			},
			retry.Limit(5),
			retry.NonRetriableErrors(ErrInvalidCommit, context.Canceled),
			retry.Backoff(backoff.BinaryExponential(500*time.Millisecond), 10*time.Second),
		)
		if err != nil && err != context.Canceled {
			log.WithError(err).Warn("Failure while ingesting, will retry")
		}

		if err := l.Unlock(); err != nil {
			log.WithError(err).Warn("Failed to release lock")
		}
	}
}
