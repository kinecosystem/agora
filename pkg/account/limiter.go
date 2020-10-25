package account

import (
	"strconv"

	"github.com/kinecosystem/agora/pkg/rate"
	"github.com/kinecosystem/agora/pkg/version"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	globalRateLimitKey = "create-account-rate-limit-global"
)

var (
	createAccountRLCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "create_account_rate_limited_global",
		Help:      "Number of globally rate limited create account requests",
	}, []string{"kin_version"})
)

type Limiter struct {
	limiter rate.Limiter
}

func NewLimiter(limiter rate.Limiter) *Limiter {
	return &Limiter{
		limiter: limiter,
	}
}

func (l *Limiter) Allow(version version.KinVersion) (bool, error) {
	allowed, err := l.limiter.Allow(globalRateLimitKey)
	if err != nil {
		return true, err
	}

	if !allowed {
		createAccountRLCounter.With(prometheus.Labels{"kin_version": strconv.Itoa(int(version))}).Add(1)
	}

	return allowed, nil
}
