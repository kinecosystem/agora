package version

import (
	"context"
	"strings"

	"github.com/kinecosystem/agora-common/kin/version"
	"github.com/kinecosystem/agora-common/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const defaultVersion = version.KinVersion4

var (
	preconditionFailedCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "precondition_failed",
		Help:      "Number of precondition failed responses from agora",
	})
)

func init() {
	preconditionFailedCounter = metrics.Register(preconditionFailedCounter).(prometheus.Counter)
}

func DisabledVersionUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if strings.Contains(info.FullMethod, "GetMinimumKinVersion") {
			return handler(ctx, req)
		}
		if strings.Contains(info.FullMethod, "Check") {
			return handler(ctx, req)
		}

		if strings.Contains(info.FullMethod, "v3") {
			preconditionFailedCounter.Inc()
			return nil, status.Error(codes.FailedPrecondition, "v3 APIs are disabled")
		}

		return handler(ctx, req)
	}
}

func DisabledVersionStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if strings.Contains(info.FullMethod, "v3") {
			preconditionFailedCounter.Inc()
			return status.Error(codes.FailedPrecondition, "v3 APIs are disabled")
		}

		return handler(srv, ss)
	}
}
