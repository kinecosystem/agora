package migration

import (
	"fmt"
	"path"

	"github.com/kinecosystem/agora-common/config"
	"github.com/kinecosystem/agora-common/config/env"
	"github.com/kinecosystem/agora-common/config/etcd"
	"github.com/kinecosystem/agora-common/config/wrapper"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	configPrefix    = "/config/agora/v1"
	configNamespace = "migration"

	kin2EnabledKey     = "kin2_enabled"
	kin2EnabledDefault = false

	kin3EnabledKey     = "kin3_enabled"
	kin3EnabledDefault = true
)

type ConfigProvider func(*conf)

type conf struct {
	kin2Enabled config.Bool
	kin3Enabled config.Bool
}

func WithEnvConfig() ConfigProvider {
	return func(c *conf) {
		c.kin2Enabled = wrapper.NewBoolConfig(env.NewConfig(fmt.Sprintf("%s_%s", configNamespace, kin2EnabledKey)), kin2EnabledDefault)
		c.kin3Enabled = wrapper.NewBoolConfig(env.NewConfig(fmt.Sprintf("%s_%s", configNamespace, kin3EnabledKey)), kin3EnabledDefault)
	}
}

func WithETCDConfigs(client *clientv3.Client) ConfigProvider {
	return func(c *conf) {
		c.kin2Enabled = etcd.NewBoolConfig(client, path.Join(configPrefix, configNamespace, kin2EnabledKey), kin2EnabledDefault)
		c.kin3Enabled = etcd.NewBoolConfig(client, path.Join(configPrefix, configNamespace, kin3EnabledKey), kin3EnabledDefault)
	}
}

func WithOverrides(kin2Enabled, kin3Enabled config.Config) ConfigProvider {
	return func(c *conf) {
		c.kin2Enabled = wrapper.NewBoolConfig(kin2Enabled, kin2EnabledDefault)
		c.kin3Enabled = wrapper.NewBoolConfig(kin3Enabled, kin3EnabledDefault)
	}
}
