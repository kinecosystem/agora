package account

import (
	"strconv"

	"github.com/kinecosystem/agora-common/kin/version"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/kinecosystem/agora/pkg/rate"
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

func init() {
	if err := registerMetrics(); err != nil {
		logrus.WithError(err).Error("failed to register account limiter metrics")
	}
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

func registerMetrics() error {
	if err := prometheus.Register(createAccountRLCounter); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			createAccountRLCounter = e.ExistingCollector.(*prometheus.CounterVec)
		} else {
			return errors.Wrap(err, "failed to register create account global rate limit counter")
		}
	}

	return nil
}
