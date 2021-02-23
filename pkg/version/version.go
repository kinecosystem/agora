package version

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/kinecosystem/agora-common/config"
	"github.com/kinecosystem/agora-common/config/env"
	"github.com/kinecosystem/agora-common/config/etcd"
	"github.com/kinecosystem/agora-common/config/wrapper"
	"github.com/kinecosystem/agora-common/kin/version"
	"github.com/kinecosystem/agora-common/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	configPrefix    = "/config/agora/v1"
	configNamespace = "version"

	defaultVersionKey     = "default"
	defaultVersionDefault = 4

	disabledVersionsKey     = "disabled"
	disabledVersionsDefault = "2,3"
)

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

type ConfigProvider func(c *conf)

type conf struct {
	log *logrus.Entry

	defaultVersion        config.Uint64
	disabledVersionString config.String
}

func (c conf) getDisabledVersions(ctx context.Context) (versions []int) {
	parts := strings.Split(c.disabledVersionString.Get(ctx), ",")
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}

		v, err := strconv.ParseUint(p, 10, 64)
		if err != nil {
			c.log.WithError(err).WithField("val", v).Error("failed to parse disabled version")
		}

		versions = append(versions, int(v))
	}

	return versions
}

func WithETCDConfig(client *clientv3.Client) ConfigProvider {
	return func(c *conf) {
		c.log = logrus.StandardLogger().WithFields(logrus.Fields{
			"type":     "version/config",
			"provider": "etcd",
		})
		c.defaultVersion = etcd.NewUint64Config(client, path.Join(configPrefix, configNamespace, defaultVersionKey), defaultVersionDefault)
		c.disabledVersionString = etcd.NewStringConfig(client, path.Join(configPrefix, configNamespace, disabledVersionsKey), disabledVersionsDefault)
	}
}

func WithEnvConfig() ConfigProvider {
	return func(c *conf) {
		c.log = logrus.StandardLogger().WithFields(logrus.Fields{
			"type":     "version/config",
			"provider": "env",
		})
		c.defaultVersion = wrapper.NewUint64Config(env.NewConfig(fmt.Sprintf("%s_%s", configNamespace, defaultVersionKey)), defaultVersionDefault)
		c.disabledVersionString = wrapper.NewStringConfig(env.NewConfig(fmt.Sprintf("%s_%s", configNamespace, disabledVersionsKey)), disabledVersionsDefault)
	}
}

func DisabledVersionUnaryServerInterceptor(c ConfigProvider) grpc.UnaryServerInterceptor {
	log := logrus.StandardLogger().WithField("type", "version/interceptor")
	var conf conf
	c(&conf)

	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		defaultVersion := version.KinVersion(conf.defaultVersion.Get(ctx))
		disabledVersions := conf.getDisabledVersions(ctx)

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

		version, err := version.GetCtxKinVersion(ctx)
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

func DisabledVersionStreamServerInterceptor(c ConfigProvider) grpc.StreamServerInterceptor {
	log := logrus.StandardLogger().WithField("type", "version/interceptor")
	var conf conf
	c(&conf)

	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		defaultVersion := version.KinVersion(conf.defaultVersion.Get(ss.Context()))
		disabledVersions := conf.getDisabledVersions(ss.Context())

		if strings.Contains(info.FullMethod, "v4") {
			for _, v := range disabledVersions {
				if v == 4 {
					preconditionFailedCounter.Inc()
					return status.Error(codes.FailedPrecondition, "unsupported kin version")
				}
			}
			return handler(srv, ss)
		}

		version, err := version.GetCtxKinVersion(ss.Context())
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

		desired, err := version.GetCtxDesiredVersion(ctx)
		if err != nil {
			log.WithError(err).Warn("failed to get desired kin version; ignoring")
			return handler(ctx, req)
		}

		switch desired {
		case version.KinVersion2, version.KinVersion3:
			actual, err := version.GetCtxKinVersion(ctx)
			if err != nil {
				log.WithError(err).Warn("failed to get kin version; ignoring")
				return handler(ctx, req)
			}

			if actual < desired {
				preconditionFailedCounter.Inc()
				return nil, status.Error(codes.FailedPrecondition, "unsupported kin version")
			}
		case version.KinVersion4:
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

		desired, err := version.GetCtxDesiredVersion(ss.Context())
		if err != nil {
			log.WithError(err).Warn("failed to get desired kin version; ignoring")
			return handler(srv, ss)
		}

		switch desired {
		case version.KinVersion2, version.KinVersion3:
			actual, err := version.GetCtxKinVersion(ss.Context())
			if err != nil {
				log.WithError(err).Warn("failed to get kin version; ignoring")
				return handler(srv, ss)
			}

			if actual < desired {
				preconditionFailedCounter.Inc()
				return status.Error(codes.FailedPrecondition, "unsupported kin version")
			}
		case version.KinVersion4:
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
