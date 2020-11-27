package migration

import (
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

var (
	OnDemandSuccessCounterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "on_demand_migration_success",
		Help:      "Number of successful on demand migrations",
	}, []string{"kin_version"})

	OnDemandFailureCounterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "on_demand_migration_failure",
		Help:      "Number of failed on demand migrations",
	}, []string{"kin_version"})

	markCompleteSuccessCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "migration_mark_complete_success",
		Help:      "Number of account migrations marked complete successfully",
	})
	markCompleteFailureCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "migration_mark_complete_failure",
		Help:      "Number of failed attempts to mark account migrations as complete",
	})

	initiateMigrationBeforeCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "initiated_migration_before_filter",
		Help:      "Number of initiate migration calls before context filtering",
	})
	initiateMigrationAfterCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "initiated_migration_after_filter",
		Help:      "Number of initiate migration calls after context filtering",
	})

	MigrationAllowedCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "migration_allowed",
		Help:      "Number of initiate migration calls allowed",
	})
	MigrationRateLimitedCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "migration_rate_limited",
		Help:      "Number of initiate migration calls rate limited",
	})
)

func init() {
	if err := registerMetrics(); err != nil {
		logrus.WithError(err).Error("failed to register migration metrics")
	}
}

func registerMetrics() error {
	if err := prometheus.Register(OnDemandSuccessCounterVec); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			OnDemandSuccessCounterVec = e.ExistingCollector.(*prometheus.CounterVec)
		} else {
			return errors.Wrap(err, "failed to register on demand migration success counter")
		}
	}

	if err := prometheus.Register(OnDemandFailureCounterVec); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			OnDemandFailureCounterVec = e.ExistingCollector.(*prometheus.CounterVec)
		} else {
			return errors.Wrap(err, "failed to register on demand migration failure counter")
		}
	}

	if err := prometheus.Register(markCompleteSuccessCounter); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			markCompleteSuccessCounter = e.ExistingCollector.(prometheus.Counter)
		} else {
			return errors.Wrap(err, "failed to register marked complete counter")
		}
	}

	if err := prometheus.Register(markCompleteFailureCounter); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			markCompleteFailureCounter = e.ExistingCollector.(prometheus.Counter)
		} else {
			return errors.Wrap(err, "failed to register marked complete counter")
		}
	}

	if err := prometheus.Register(initiateMigrationBeforeCounter); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			initiateMigrationBeforeCounter = e.ExistingCollector.(prometheus.Counter)
			return nil
		}
		return errors.Wrap(err, "failed to register initiate migration before filter counter")
	}

	if err := prometheus.Register(initiateMigrationAfterCounter); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			initiateMigrationAfterCounter = e.ExistingCollector.(prometheus.Counter)
		} else {
			return errors.Wrap(err, "failed to register initiate migration after filter counter")
		}
	}

	if err := prometheus.Register(MigrationAllowedCounter); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			MigrationAllowedCounter = e.ExistingCollector.(prometheus.Counter)
		} else {
			return errors.Wrap(err, "failed to register migration allowed counter")
		}
	}

	if err := prometheus.Register(MigrationRateLimitedCounter); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			MigrationRateLimitedCounter = e.ExistingCollector.(prometheus.Counter)
		} else {
			return errors.Wrap(err, "failed to register migration rate limited counter")
		}
	}

	if err := prometheus.Register(successCounter); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			successCounter = e.ExistingCollector.(prometheus.Counter)
		} else {
			return errors.Wrap(err, "failed to register migration success counter (offline)")
		}

	}

	if err := prometheus.Register(multiSigCounter); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			multiSigCounter = e.ExistingCollector.(prometheus.Counter)
		} else {
			return errors.Wrap(err, "failed to register migration multisig counter (offline)")
		}
	}

	if err := prometheus.Register(burnedCounter); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			burnedCounter = e.ExistingCollector.(prometheus.Counter)
		} else {
			return errors.Wrap(err, "failed to register migration burned counter (offline)")
		}
	}

	if err := prometheus.Register(failureCounter); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			failureCounter = e.ExistingCollector.(prometheus.Counter)
		} else {
			return errors.Wrap(err, "failed to register migration failed counter (offline)")
		}
	}

	return nil
}
