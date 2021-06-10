package server

import (
	"fmt"

	"github.com/kinecosystem/agora-common/config"
	"github.com/kinecosystem/agora-common/config/env"
	"github.com/kinecosystem/agora-common/config/wrapper"
)

const (
	configNamespace = "account/server"

	resolveConsistencyCheckRate        = "resolve_consistency_check_rate"
	resolveConsistencyCheckRateDefault = 0.1
)

type ConfigProvider func(*conf)

type conf struct {
	resolveConsistencyCheckRate config.Float64
}

func WithEnvConfig() ConfigProvider {
	return func(c *conf) {
		c.resolveConsistencyCheckRate = wrapper.NewFloat64Config(env.NewConfig(fmt.Sprintf("%s_%s", configNamespace, resolveConsistencyCheckRate)), resolveConsistencyCheckRateDefault)
	}
}

func WithOverrides(resolveShortcutsEnabled, resolveConsistencyCheckRate config.Config) ConfigProvider {
	return func(c *conf) {
		c.resolveConsistencyCheckRate = wrapper.NewFloat64Config(resolveConsistencyCheckRate, resolveConsistencyCheckRateDefault)
	}
}
