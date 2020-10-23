package transaction

import (
	"fmt"
	"strconv"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/kinecosystem/agora/pkg/rate"
)

const (
	globalRateLimitKey    = "submit-transaction-rate-limit-global"
	appRateLimitKeyFormat = "submit-transaction-rate-limit-app-%d"
)

var (
	submitRLCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "submit_transaction_rate_limited_global",
		Help:      "Number of globally rate limited create account requests",
	})
	submitRLAppCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "submit_transaction_rate_limited_app",
		Help:      "Number of app rate limited create account requests",
	}, []string{"app_index"})
)

// Limiter limits transaction based on app index.
//
// If no app index is provided, only the global rate limit applies.
type Limiter struct {
	global rate.Limiter
	app    rate.Limiter
}

// NewLimiter creates a new TransactionLimiter.
func NewLimiter(ctor rate.LimiterCtor, globalLimit, appLimit int) *Limiter {
	return &Limiter{
		global: ctor(globalLimit),
		app:    ctor(appLimit),
	}
}

// Allow returns whether or not a transaction for a given app index
// is allowed to be processed.
func (t *Limiter) Allow(appIndex int) (bool, error) {
	// note: it is important that we check the app rate limit before
	//       the global limit, as it is the smaller of the two. Since
	//       checking a limiter actually consumes the rate, we don't
	//       want to consume the global rate if app limiter kicks in.
	if appIndex > 0 {
		allowed, err := t.app.Allow(fmt.Sprintf(appRateLimitKeyFormat, appIndex))
		if err != nil {
			return true, err
		}
		if !allowed {
			submitRLAppCounter.WithLabelValues(strconv.Itoa(appIndex))
			return allowed, err
		}
	}

	allowed, err := t.global.Allow(globalRateLimitKey)
	if err != nil {
		return true, err
	} else if !allowed {
		submitRLCounter.Inc()
	}

	return allowed, err
}

func registerMetrics() (err error) {
	if err := prometheus.Register(submitRLCounter); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			submitRLCounter = e.ExistingCollector.(prometheus.Counter)
			return nil
		}
		return errors.Wrap(err, "failed to register submit tx global rate limit counter")
	}
	if err := prometheus.Register(submitRLAppCounter); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); !ok {
			submitRLAppCounter = e.ExistingCollector.(*prometheus.CounterVec)
			return nil
		}
		return errors.Wrap(err, "failed to register submit tx app rate limit counter")
	}

	return nil
}
