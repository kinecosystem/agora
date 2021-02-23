package server

import (
	"github.com/kinecosystem/agora-common/metrics"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/kinecosystem/agora/pkg/rate"
)

const (
	globalRateLimitKey = "create-account-rate-limit-global"
)

var (
	createAccountRLCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "create_account_rate_limited_global",
		Help:      "Number of globally rate limited create account requests",
	})
)

type Limiter struct {
	limiter rate.Limiter
}

func init() {
	createAccountRLCounter = metrics.Register(createAccountRLCounter).(prometheus.Counter)
}

func NewLimiter(limiter rate.Limiter) *Limiter {
	return &Limiter{
		limiter: limiter,
	}
}

func (l *Limiter) Allow() (bool, error) {
	allowed, err := l.limiter.Allow(globalRateLimitKey)
	if err != nil {
		return true, err
	}

	if !allowed {
		createAccountRLCounter.Inc()
	}

	return allowed, nil
}
