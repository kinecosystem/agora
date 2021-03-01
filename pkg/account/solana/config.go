package solana

import (
	"fmt"
	"path"

	"github.com/kinecosystem/agora-common/config"
	"github.com/kinecosystem/agora-common/config/env"
	"github.com/kinecosystem/agora-common/config/etcd"
	"github.com/kinecosystem/agora-common/config/wrapper"
	"go.etcd.io/etcd/clientv3"
)

const (
	configPrefix    = "/config/agora/v1"
	configNamespace = "account/server"

	resolveShortcutsEnabled        = "resolve_shortcuts_enabled"
	resolveShortcutsEnabledDefault = true

	resolveConsistencyCheckRate        = "resolve_consistency_check_rate"
	resolveConsistencyCheckRateDefault = 0.1
)

type ConfigProvider func(*conf)

type conf struct {
	resolveShortcutsEnabled     config.Bool
	resolveConsistencyCheckRate config.Float64
}

func WithEnvConfig() ConfigProvider {
	return func(c *conf) {
		c.resolveShortcutsEnabled = wrapper.NewBoolConfig(env.NewConfig(fmt.Sprintf("%s_%s", configNamespace, resolveShortcutsEnabled)), resolveShortcutsEnabledDefault)
		c.resolveConsistencyCheckRate = wrapper.NewFloat64Config(env.NewConfig(fmt.Sprintf("%s_%s", configNamespace, resolveConsistencyCheckRate)), resolveConsistencyCheckRateDefault)
	}
}

func WithETCDConfigs(client *clientv3.Client) ConfigProvider {
	return func(c *conf) {
		c.resolveShortcutsEnabled = etcd.NewBoolConfig(client, path.Join(configPrefix, configNamespace, resolveShortcutsEnabled), resolveShortcutsEnabledDefault)
		c.resolveConsistencyCheckRate = etcd.NewFloat64Config(client, path.Join(configPrefix, configNamespace, resolveConsistencyCheckRate), resolveConsistencyCheckRateDefault)
	}
}

func WithOverrides(resolveShortcutsEnabled, resolveConsistencyCheckRate config.Config) ConfigProvider {
	return func(c *conf) {
		c.resolveShortcutsEnabled = wrapper.NewBoolConfig(resolveShortcutsEnabled, resolveShortcutsEnabledDefault)
		c.resolveConsistencyCheckRate = wrapper.NewFloat64Config(resolveConsistencyCheckRate, resolveConsistencyCheckRateDefault)
	}
}
