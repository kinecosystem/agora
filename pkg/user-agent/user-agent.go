package user_agent

import (
	"context"
	"github.com/kinecosystem/agora-common/headers"
	"github.com/kinecosystem/agora-common/metrics"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
)

var (
	preconditionFailedCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "precondition_failed",
		Help:      "Number of precondition failed responses from agora",
	})
)
const (
	userAgentHeader = "kin-user-agent"
)

func init() {
	preconditionFailedCounter = metrics.Register(preconditionFailedCounter).(prometheus.Counter)
}

func DisabledUserAgentUnaryServerInterceptor(sdkFilter string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if !strings.Contains(info.FullMethod, "CreateAccount") {
			return handler(ctx, req)
		}

		sdks := strings.Split(sdkFilter, ",")

		if sdkFilter == "" || len(sdks) < 1 {
			return handler(ctx, req)
		}

		userAgent, err := headers.GetASCIIHeaderByName(ctx, userAgentHeader)
		if err != nil {
			log.Warnf("Error getting userAgent %s:", err)
			return handler(ctx, req)
		}

		if userAgent == "" {
			return handler(ctx, req)
		}

		for _, sdk := range sdks {
			if strings.Contains(userAgent, sdk) {
				preconditionFailedCounter.Inc()
				return nil, status.Errorf(codes.FailedPrecondition, "CreateAccount is disabled for SDK Version %s. Please update to the latest SDK.", userAgent)
			}
		}

		return handler(ctx, req)
	}
}
