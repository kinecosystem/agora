package account

import (
	"fmt"
	"strconv"

	"github.com/kinecosystem/agora-common/metrics"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/kinecosystem/agora/pkg/rate"
)

const (
	globalRateLimitKey    = "create-account-rate-limit-global"
	appRateLimitKeyFormat = "create-account-rate-limit-app-%d"
)

var (
	submitRLCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "create_account_rate_limited_global",
		Help:      "Number of globally rate limited create account requests",
	})
	submitRLAppCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "create_account_rate_limited_app",
		Help:      "Number of app rate limited create account requests",
	}, []string{"app_index"})
)

// Limiter limits account creations based on app index.
//
// If no app index is provided, only the global rate limit applies.
type Limiter struct {
	global rate.Limiter
	app    rate.Limiter
}

// NewLimiter creates a new Limiter.
func NewLimiter(ctor rate.LimiterCtor, globalLimit, appLimit int) *Limiter {
	return &Limiter{
		global: ctor(globalLimit),
		app:    ctor(appLimit),
	}
}

// Allow returns whether or not an account creation for a given app index
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
			submitRLAppCounter.WithLabelValues(strconv.Itoa(appIndex)).Inc()
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

func init() {
	submitRLCounter = metrics.Register(submitRLCounter).(prometheus.Counter)
	submitRLAppCounter = metrics.Register(submitRLAppCounter).(*prometheus.CounterVec)
}
