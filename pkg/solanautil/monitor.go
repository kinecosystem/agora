package solanautil

import (
	"context"
	"crypto/ed25519"
	"time"

	"github.com/kinecosystem/agora-common/metrics"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/mr-tron/base58/base58"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

var nativeBalance = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "agora",
	Name:      "native_balance",
	Help:      "Native balance of a wallet (polled every minute, in lamports)",
}, []string{"address"})

func init() {
	nativeBalance = metrics.Register(nativeBalance).(*prometheus.GaugeVec)
}

func MonitorAccount(ctx context.Context, sc solana.Client, pub ed25519.PublicKey) {
	log := logrus.StandardLogger().WithFields(logrus.Fields{
		"method":  "MonitorAccount",
		"account": base58.Encode(pub),
	})

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Minute):
			}

			balance, err := sc.GetBalance(pub)
			if err != nil {
				log.WithError(err).Warn("failed to query account balance")
			}

			nativeBalance.WithLabelValues(base58.Encode(pub)).Set(float64(balance))
		}
	}()
}
