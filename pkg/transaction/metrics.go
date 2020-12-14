package transaction

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

var (
	SubmitTransactionCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "submit_transaction",
		Help:      "Number of submit transaction requests",
	}, []string{"api_version"})
)

func init() {
	if err := prometheus.Register(SubmitTransactionCounter); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			submitRLAppCounter = e.ExistingCollector.(*prometheus.CounterVec)
		} else {
			logrus.WithError(err).Error("failed to register submit transaction counter")
		}
	}
}
