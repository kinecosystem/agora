package redis

import (
	"context"
	"encoding/base64"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/agora-common/metrics"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/kinecosystem/agora/pkg/events"
	"github.com/kinecosystem/agora/pkg/events/eventspb"
)

const TransactionChannel = "txns"

var (
	submitHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "agora",
		Name:      "redis_events_submit_duration_seconds",
	}, []string{"channel"})
	visibilityHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "agora",
		Name:      "redis_events_visibility_seconds",
		Help:      "The time for an event to become 'visible' by the subscribers",
	}, []string{"channel"})
)

func init() {
	submitHistogram = metrics.Register(submitHistogram).(*prometheus.HistogramVec)
	visibilityHistogram = metrics.Register(visibilityHistogram).(*prometheus.HistogramVec)
}

type PubSub struct {
	log     *logrus.Entry
	channel string
	rdb     *redis.Client
	ps      *redis.PubSub
	hooks   []events.Hook
}

func New(
	rdb *redis.Client,
	channel string,
	hooks ...events.Hook,
) (*PubSub, error) {
	ps := rdb.Subscribe(channel)
	_, err := ps.Receive()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to subscribe to %s", channel)
	}

	p := &PubSub{
		log: logrus.StandardLogger().WithFields(logrus.Fields{
			"type":    "events/redis",
			"channel": channel,
		}),
		rdb:     rdb,
		channel: channel,
		ps:      ps,
		hooks:   hooks,
	}

	go p.watch()
	return p, nil
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

	raw, err := proto.Marshal(event)
	if err != nil {
		return errors.Wrap(err, "failed to marshal event")
	}

	start := time.Now()
	result := p.rdb.Publish(p.channel, base64.StdEncoding.EncodeToString(raw))
	submitHistogram.WithLabelValues(p.channel).Observe(time.Since(start).Seconds())

	return result.Err()
}

func (p *PubSub) Close() error {
	return p.ps.Close()
}

func (p *PubSub) watch() {
	log := p.log.WithField("method", "watch")
	eventCh := p.ps.Channel()

	for msg := range eventCh {
		raw, err := base64.StdEncoding.DecodeString(msg.Payload)
		if err != nil {
			log.WithError(err).Warn("invalid message in channel, ignoring")
			continue
		}

		m := &eventspb.Event{}
		if err := proto.Unmarshal(raw, m); err != nil {
			log.WithError(err).WithError(err).Warn("invalid message in channel, ignoring")
			continue
		}

		// todo: If nodes are struggling to keep up, we can drop events that are too old.
		//       However, the redis pubsub client itself already has buffering semantics,
		//       so it's unlikely we'd ever hit that case.
		visibilityHistogram.WithLabelValues(p.channel).Observe(time.Since(m.SubmissionTime.AsTime()).Seconds())

		for _, h := range p.hooks {
			h(m)
		}
	}
}
