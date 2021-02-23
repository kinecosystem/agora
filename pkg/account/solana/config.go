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
)

type ConfigProvider func(*conf)

type conf struct {
	resolveShortcutsEnabled config.Bool
}

func WithEnvConfig() ConfigProvider {
	return func(c *conf) {
		c.resolveShortcutsEnabled = wrapper.NewBoolConfig(env.NewConfig(fmt.Sprintf("%s_%s", configNamespace, resolveShortcutsEnabled)), resolveShortcutsEnabledDefault)
	}
}

func WithETCDConfigs(client *clientv3.Client) ConfigProvider {
	return func(c *conf) {
		c.resolveShortcutsEnabled = etcd.NewBoolConfig(client, path.Join(configPrefix, configNamespace, resolveShortcutsEnabled), resolveShortcutsEnabledDefault)
	}
}

func WithOverrides(resolveShortcutsEnabled config.Config) ConfigProvider {
	return func(c *conf) {
		c.resolveShortcutsEnabled = wrapper.NewBoolConfig(resolveShortcutsEnabled, resolveShortcutsEnabledDefault)
	}
}
