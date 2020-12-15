package version

import (
	"context"
	"strconv"
	"strings"

	"github.com/kinecosystem/agora-common/headers"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type KinVersion uint16

const (
	KinVersionUnknown KinVersion = iota
	KinVersionReserved
	KinVersion2
	KinVersion3
	KinVersion4
)

const (
	KinVersionHeader        = "kin-version"
	DesiredKinVersionHeader = "desired-kin-version"
	minVersion              = KinVersion2
	maxVersion              = KinVersion4
	defaultVersion          = KinVersion3
)

var (
	preconditionFailedCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "precondition_failed",
		Help:      "Number of precondition failed responses from agora",
	})
)

func init() {
	if err := registerMetrics(); err != nil {
		logrus.WithError(err).Error("failed to register precondition failed counter")
	}
}

// GetCtxKinVersion determines which version of Kin to use based on the headers in the provided context.
func GetCtxKinVersion(ctx context.Context) (version KinVersion, err error) {
	val, err := headers.GetASCIIHeaderByName(ctx, KinVersionHeader)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get kin version header")
	}

	if len(val) == 0 {
		return defaultVersion, nil
	}

	i, err := strconv.Atoi(val)
	if err != nil {
		return 0, errors.Wrap(err, "could not parse integer version from string")
	}

	if i < int(minVersion) || i > int(maxVersion) {
		return 0, errors.Wrap(err, "invalid kin version")
	}

	return KinVersion(i), nil
}

// GetCtxDesiredVersion determines which version of Kin the requestor whiches to have enforced.
func GetCtxDesiredVersion(ctx context.Context) (version KinVersion, err error) {
	val, err := headers.GetASCIIHeaderByName(ctx, DesiredKinVersionHeader)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get desired kin version header")
	}

	if len(val) == 0 {
		return GetCtxKinVersion(ctx)
	}

	i, err := strconv.Atoi(val)
	if err != nil {
		return 0, errors.Wrap(err, "could not parse integer version from string")
	}

	if i < int(minVersion) || i > int(maxVersion) {
		return 0, errors.Wrap(err, "invalid desired kin version")
	}

	return KinVersion(i), nil
}

func DisabledVersionUnaryServerInterceptor(defaultVersion KinVersion, disabledVersions []int) grpc.UnaryServerInterceptor {
	log := logrus.StandardLogger().WithField("type", "version/interceptor")
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if strings.Contains(info.FullMethod, "GetMinimumKinVersion") {
			return handler(ctx, req)
		}
		if strings.Contains(info.FullMethod, "Check") {
			return handler(ctx, req)
		}
		if strings.Contains(info.FullMethod, "v4") {
			for _, v := range disabledVersions {
				if v == 4 {
					preconditionFailedCounter.Inc()
					return nil, status.Error(codes.FailedPrecondition, "unsupported kin version")
				}
			}
			return handler(ctx, req)
		}

		version, err := GetCtxKinVersion(ctx)
		if err != nil {
			log.WithError(err).Warn("failed to get kin version; reverting to default")
			version = defaultVersion
		}

		for i := range disabledVersions {
			if int(version) == disabledVersions[i] {
				preconditionFailedCounter.Inc()
				return nil, status.Error(codes.FailedPrecondition, "unsupported kin version")
			}
		}

		return handler(ctx, req)
	}
}

func DisabledVersionStreamServerInterceptor(defaultVersion KinVersion, disabledVersions []int) grpc.StreamServerInterceptor {
	log := logrus.StandardLogger().WithField("type", "version/interceptor")
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if strings.Contains(info.FullMethod, "v4") {
			for _, v := range disabledVersions {
				if v == 4 {
					preconditionFailedCounter.Inc()
					return status.Error(codes.FailedPrecondition, "unsupported kin version")
				}
			}
			return handler(srv, ss)
		}

		version, err := GetCtxKinVersion(ss.Context())
		if err != nil {
			log.WithError(err).Warn("failed to get kin version; reverting to default")
			version = defaultVersion
		}

		for i := range disabledVersions {
			if int(version) == disabledVersions[i] {
				preconditionFailedCounter.Inc()
				return status.Error(codes.FailedPrecondition, "unsupported kin version")
			}
		}

		return handler(srv, ss)
	}
}

// MinVersionUnaryServerInterceptor prevents versions below the minimum
// version from accessing lower version APIs.
func MinVersionUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	log := logrus.StandardLogger().WithField("type", "version/interceptor")
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if strings.Contains(info.FullMethod, "GetMinimumKinVersion") {
			return handler(ctx, req)
		}

		desired, err := GetCtxDesiredVersion(ctx)
		if err != nil {
			log.WithError(err).Warn("failed to get desired kin version; ignoring")
			return handler(ctx, req)
		}

		switch desired {
		case KinVersion2, KinVersion3:
			actual, err := GetCtxKinVersion(ctx)
			if err != nil {
				log.WithError(err).Warn("failed to get kin version; ignoring")
				return handler(ctx, req)
			}

			if actual < desired {
				preconditionFailedCounter.Inc()
				return nil, status.Error(codes.FailedPrecondition, "unsupported kin version")
			}
		case KinVersion4:
			if !strings.Contains(info.FullMethod, "v4") {
				preconditionFailedCounter.Inc()
				return nil, status.Error(codes.FailedPrecondition, "version not supported")
			}
		default:
			log.WithField("version", desired).Warn("unhandled kin version; ignoring")
		}

		return handler(ctx, req)
	}
}

// MinVersionStreamServerInterceptor prevents versions below the minimum
// version from accessing lower version APIs.
func MinVersionStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		log := logrus.StandardLogger().WithField("type", "version/interceptor")

		desired, err := GetCtxDesiredVersion(ss.Context())
		if err != nil {
			log.WithError(err).Warn("failed to get desired kin version; ignoring")
			return handler(srv, ss)
		}

		switch desired {
		case KinVersion2, KinVersion3:
			actual, err := GetCtxKinVersion(ss.Context())
			if err != nil {
				log.WithError(err).Warn("failed to get kin version; ignoring")
				return handler(srv, ss)
			}

			if actual < desired {
				preconditionFailedCounter.Inc()
				return status.Error(codes.FailedPrecondition, "unsupported kin version")
			}
		case KinVersion4:
			if !strings.Contains(info.FullMethod, "v4") {
				preconditionFailedCounter.Inc()
				return status.Error(codes.FailedPrecondition, "version not supported")
			}
		default:
			log.WithField("version", desired).Warn("unhandled kin version; ignoring")
		}

		return handler(srv, ss)
	}
}

func registerMetrics() error {
	if err := prometheus.Register(preconditionFailedCounter); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			preconditionFailedCounter = e.ExistingCollector.(prometheus.Counter)
		} else {
			return err
		}
	}

	return nil
}
