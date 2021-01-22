package migration

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/agora-common/retry"
	"github.com/kinecosystem/agora-common/retry/backoff"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/taskqueue"
	"github.com/kinecosystem/agora-common/taskqueue/model/task"
	"github.com/kinecosystem/go/strkey"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	xrate "golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	migrationpb "github.com/kinecosystem/agora/pkg/migration/proto"
)

const (
	MigrationQueueName         = "migration-queue"
	MigrationQueueBurnedName   = "migration-queue-burned"
	MigrationQueueMultisigName = "migration-queue-multisig"
)

var (
	zeroAccount    ed25519.PublicKey
	successCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "offline_migrate_account_success",
		Help:      "Number of successfully migrated accounts (offline migrator)",
	})
	multiSigCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "offline_migrate_multisigs",
		Help:      "Number of multisig wallets encountered while migrating accounts (offline migrator)",
	})
	burnedCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "offline_migrate_burned",
		Help:      "Number of multisig wallets encountered while migrating accounts (offline migrator)",
	})
	failureCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "offline_migrate_account_failures",
		Help:      "Number of failures while migrating accounts (offline migrator)",
	})
)

type Processor struct {
	log               *logrus.Entry
	m                 Migrator
	processor         taskqueue.Processor
	multisigSubmitter taskqueue.Submitter
	burnedSubmitter   taskqueue.Submitter

	mu           sync.RWMutex
	localLimiter *xrate.Limiter
}

func NewProcessor(
	queueCtr taskqueue.ProcessorCtor,
	burnedQueueSubmitter taskqueue.Submitter,
	multisigQueueSubmitter taskqueue.Submitter,
	m Migrator,
	localLimiter *xrate.Limiter,
) (p *Processor, err error) {
	p = &Processor{
		log:               logrus.WithField("type", "migration/processor"),
		m:                 m,
		multisigSubmitter: multisigQueueSubmitter,
		burnedSubmitter:   burnedQueueSubmitter,
		localLimiter:      localLimiter,
	}

	p.processor, err = queueCtr(p.queueHandler)
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (p *Processor) Write(ctx context.Context, account ed25519.PublicKey, ignoreZeroBalance bool) error {
	if len(account) != ed25519.PublicKeySize || bytes.Equal(account, zeroAccount) {
		return errors.Errorf("invalid account")
	}

	item := &migrationpb.QueueRequest_QueueItem{
		Key:               account,
		IgnoreZeroBalance: ignoreZeroBalance,
	}

	raw, err := proto.Marshal(item)
	if err != nil {
		return errors.Wrap(err, "failed to marshal item")
	}

	return p.processor.Submit(ctx, &task.Message{
		TypeName: proto.MessageName(item),
		RawValue: raw,
	})
}

func (p *Processor) Shutdown() {
	p.processor.Shutdown()
}

func (p *Processor) queueHandler(ctx context.Context, msg *task.Message) error {
	log := p.log.WithField("method", "queueHandler")

	item := &migrationpb.QueueRequest_QueueItem{}
	if msg.TypeName != proto.MessageName(item) {
		log.WithField("type_name", msg.TypeName).Warn("Unsupported message type")
		return errors.New("unsupported message type")
	}

	if err := proto.Unmarshal(msg.RawValue, item); err != nil {
		log.WithField("raw_value", base64.StdEncoding.EncodeToString(msg.RawValue)).Warn("Invalid raw value")
		return errors.New("invalid account key")
	}

	account := item.Key
	log.WithField("account", strkey.MustEncode(strkey.VersionByteAccountID, account)).Trace("Processing")

	// We use a two tiered approach to rate limiting, since we don't have
	// a utility for a global rate limiter that supports proper "wait".
	//
	// Using a local limiter allows us to more efficiently rate limit our
	// processing without propagating errors up to the task processor, which
	// would in turn increase the receive count of a task, potentially putting
	// the message into the DLQ.
	//
	// In the event that we have more than one migrator (which would primarily
	// occur due to reaching a bottleneck with one limiter), we can re-tune the
	// local rate limiters, and use a global rate limiter as a backup.
	//
	// Note: since the throughput is likely to be bottlenecked by IO / submission
	// times, we can most likely run on a single migrator with high concurrency.
	p.mu.RLock()
	localLimiter := p.localLimiter
	p.mu.RUnlock()

	if err := localLimiter.Wait(ctx); err != nil {
		log.WithError(err).Warn("failed to wait for local limiter")
		return errors.Wrap(err, "failed to wait for local limiter")
	}

	_, err := retry.Retry(
		func() error {
			err := p.m.InitiateMigration(ctx, account, item.IgnoreZeroBalance, solana.CommitmentMax)
			switch err {
			case nil, ErrMultisig, ErrBurned, ErrNotFound, context.Canceled:
			default:
				log.WithError(err).Warn("error running migration, retrying")
			}

			return err
		},
		retry.Limit(3),
		retry.NonRetriableErrors(ErrMultisig, ErrBurned, ErrNotFound, context.Canceled),
		retry.BackoffWithJitter(backoff.BinaryExponential(100*time.Millisecond), 5*time.Second, 0.1),
	)

	if err != nil {
		failureCounter.Inc()
		log.WithError(err).Warn("failed to migrate account")
	} else {
		successCounter.Inc()
	}

	if err == ErrMultisig {
		multiSigCounter.Inc()
		return p.multisigSubmitter.Submit(ctx, msg)
	} else if err == ErrBurned {
		burnedCounter.Inc()
		return p.burnedSubmitter.Submit(ctx, msg)
	}

	return err
}

func (p *Processor) Queue(ctx context.Context, req *migrationpb.QueueRequest) (*migrationpb.VoidResponse, error) {
	tasks := make([]*task.Message, len(req.Items))

	for i := 0; i < len(req.Items); i++ {
		if len(req.Items[i].Key) != ed25519.PublicKeySize || bytes.Equal(req.Items[i].Key, zeroAccount) {
			return nil, status.Errorf(codes.InvalidArgument, "empty key at %d", i)
		}

		raw, err := proto.Marshal(req.Items[i])
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "failed to marshal item")
		}

		tasks[i] = &task.Message{
			TypeName: proto.MessageName(req.Items[i]),
			RawValue: raw,
		}
	}

	if err := p.processor.SubmitBatch(ctx, tasks); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to submit batch: %v", err)
	}

	return &migrationpb.VoidResponse{}, nil
}

func (p *Processor) SetState(_ context.Context, req *migrationpb.SetStateRequest) (*migrationpb.VoidResponse, error) {
	switch req.State {
	case migrationpb.SetStateRequest_RUNNING:
		p.processor.Start()
	case migrationpb.SetStateRequest_STOPPED:
		p.processor.Pause()
	default:
		return nil, status.Error(codes.InvalidArgument, "")
	}

	return &migrationpb.VoidResponse{}, nil
}

func (p *Processor) SetRateLimit(_ context.Context, req *migrationpb.SetRateLimitRequest) (*migrationpb.VoidResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.localLimiter = xrate.NewLimiter(xrate.Limit(req.Rate), 1)

	return &migrationpb.VoidResponse{}, nil
}
