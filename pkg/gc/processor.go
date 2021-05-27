package gc

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/agora-common/metrics"
	"github.com/kinecosystem/agora-common/retry"
	"github.com/kinecosystem/agora-common/retry/backoff"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/kinecosystem/agora-common/taskqueue"
	"github.com/kinecosystem/agora-common/taskqueue/model/task"
	"github.com/mr-tron/base58"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go/strkey"
	xrate "golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gcpb "github.com/kinecosystem/agora/pkg/gc/proto"
	"github.com/kinecosystem/agora/pkg/solanautil"
	"github.com/kinecosystem/agora/pkg/transaction/history"
)

const (
	GCQueueName = "garbage-collector-queue"
)

var (
	zeroAccount ed25519.PublicKey = make([]byte, ed25519.PublicKeySize)

	processedCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "offline_gc_processed",
		Help:      "Number of account processed for garbage collection (offline collector)",
	})
	successCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "offline_gc_success",
		Help:      "Number of successfully garbage collected accounts (offline collector)",
	})
	failureCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "offline_gc_failures",
		Help:      "Number of failures while garbage collecting accounts (offline collector)",
	})
)

type Processor struct {
	log        *logrus.Entry
	processor  taskqueue.Processor
	hist       history.Reader
	sc         solana.Client
	tc         *token.Client
	subsidizer ed25519.PrivateKey

	mu           sync.RWMutex
	checkHistory bool
	localLimiter *xrate.Limiter
}

func NewProcessor(
	queueCtr taskqueue.ProcessorCtor,
	hist history.Reader,
	sc solana.Client,
	tc *token.Client,
	subsidizer ed25519.PrivateKey,
	localLimiter *xrate.Limiter,
) (p *Processor, err error) {
	p = &Processor{
		log:          logrus.WithField("type", "gc/processor"),
		hist:         hist,
		sc:           sc,
		tc:           tc,
		subsidizer:   subsidizer,
		localLimiter: localLimiter,
	}

	p.processor, err = queueCtr(p.queueHandler)
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (p *Processor) queueHandler(ctx context.Context, msg *task.Message) error {
	log := p.log.WithField("method", "queueHandler")

	item := &gcpb.QueueRequest_QueueItem{}
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
			closed, err := p.maybeCloseAccount(account)
			if err == nil && closed {
				successCounter.Inc()
			}

			return err
		},
		retry.Limit(3),
		retry.NonRetriableErrors(context.Canceled),
		retry.BackoffWithJitter(backoff.BinaryExponential(100*time.Millisecond), 5*time.Second, 0.1),
	)

	if err != nil {
		failureCounter.Inc()
		log.WithError(err).Warn("failed to migrate account")
	} else {
		processedCounter.Inc()
	}

	return err
}

func (p *Processor) maybeCloseAccount(account ed25519.PublicKey) (bool, error) {
	a, err := p.tc.GetAccount(account, solana.CommitmentSingle)
	if err == token.ErrAccountNotFound || err == token.ErrInvalidTokenAccount {
		return false, nil
	} else if err != nil {
		return false, errors.Wrap(err, "failed to load account")
	}

	if a.Amount != 0 || !bytes.Equal(a.CloseAuthority, p.subsidizer.Public().(ed25519.PublicKey)) {
		return false, nil
	}

	p.mu.Lock()
	checkHistory := p.checkHistory
	p.mu.Unlock()

	if checkHistory {
		entries, err := p.hist.GetAccountTransactions(context.Background(), strkey.MustEncode(strkey.VersionByteAccountID, account), &history.ReadOptions{
			Limit: 1,
		})
		if err != nil {
			return false, errors.Wrapf(err, "failed to load transactions for account: %s", base58.Encode(account))
		}
		if len(entries) > 0 {
			return false, nil
		}
	}

	tx := solana.NewTransaction(
		p.subsidizer.Public().(ed25519.PublicKey),
		token.CloseAccount(
			account,
			p.subsidizer.Public().(ed25519.PublicKey),
			p.subsidizer.Public().(ed25519.PublicKey),
		),
	)
	bh, err := p.sc.GetRecentBlockhash()
	if err != nil {
		return false, errors.Wrap(err, "failed to get recent blockhash")
	}
	tx.SetBlockhash(bh)
	if err := tx.Sign(p.subsidizer); err != nil {
		return false, errors.Wrap(err, "failed to sign close transaction")
	}

	// Choose single for better throughput. It's not critical if it fails later on.
	_, stat, err := p.sc.SubmitTransaction(tx, solana.CommitmentSingle)
	if err != nil {
		return false, errors.Wrapf(err, "failed to submit close for account: %s", base58.Encode(account))
	} else if stat.ErrorResult != nil {
		// The account doesn't exist, or is not a token account. Either way, we can mark it as
		// successfully processed.
		if solanautil.IsInvalidAccountDataError(stat.ErrorResult) {
			return false, nil
		}

		return false, errors.Wrapf(stat.ErrorResult, "failed to submit close for account: %s", base58.Encode(account))
	}

	return true, nil
}

func (p *Processor) Queue(ctx context.Context, req *gcpb.QueueRequest) (*gcpb.VoidResponse, error) {
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

	return &gcpb.VoidResponse{}, nil
}

func (p *Processor) SetState(_ context.Context, req *gcpb.SetStateRequest) (*gcpb.VoidResponse, error) {
	switch req.State {
	case gcpb.SetStateRequest_RUNNING:
		p.processor.Start()
	case gcpb.SetStateRequest_STOPPED:
		p.processor.Pause()
	default:
		return nil, status.Error(codes.InvalidArgument, "")
	}

	return &gcpb.VoidResponse{}, nil
}

func (p *Processor) SetRateLimit(_ context.Context, req *gcpb.SetRateLimitRequest) (*gcpb.VoidResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.localLimiter = xrate.NewLimiter(xrate.Limit(req.Rate), 1)

	return &gcpb.VoidResponse{}, nil
}

func (p *Processor) SetHistoryCheck(_ context.Context, req *gcpb.SetHistoryCheckRequest) (*gcpb.VoidResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.checkHistory = req.Enabled

	return &gcpb.VoidResponse{}, nil
}

func (p *Processor) Shutdown() {
	p.processor.Shutdown()
}

func init() {
	processedCounter = metrics.Register(processedCounter).(prometheus.Counter)
	successCounter = metrics.Register(successCounter).(prometheus.Counter)
	failureCounter = metrics.Register(failureCounter).(prometheus.Counter)
}
