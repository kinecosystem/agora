package environment

import (
	"context"
	"fmt"
	"os"

	kinkeypair "github.com/kinecosystem/go/keypair"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/kinecosystem/agora/pkg/keypair"
)

const StoreType = "environment"

const (
	prefixEnvVarKey     = "KEYSTORE_ENV_PREFIX"
	defaultEnvVarPrefix = "KEYPAIR"
	envVarFormat        = "%s_%s"
)

type store struct {
	envVarPrefix string
}

func init() {
	keypair.RegisterStoreConstructor(StoreType, newStore)
}

func newStore() (keypair.Keystore, error) {
	prefix := os.Getenv(prefixEnvVarKey)
	if len(prefix) == 0 {
		logrus.WithField("type", "keypair/environment").Infof("environment variable prefix not configured, using default (%s)", defaultEnvVarPrefix)
		prefix = defaultEnvVarPrefix
	}
	return &store{
		envVarPrefix: prefix,
	}, nil
}

// Put implements Keystore.Put
func (s *store) Put(ctx context.Context, id string, full *kinkeypair.Full) error {
	key := fmt.Sprintf(envVarFormat, s.envVarPrefix, id)
	val := os.Getenv(key)
	if len(val) != 0 {
		return keypair.ErrKeypairAlreadyExists
	}

	return os.Setenv(key, full.Seed())
}

// Get implements Keystore.Get
func (s *store) Get(ctx context.Context, id string) (*kinkeypair.Full, error) {
	val := os.Getenv(fmt.Sprintf(envVarFormat, s.envVarPrefix, id))
	if len(val) == 0 {
		return nil, keypair.ErrKeypairNotFound
	}

	kp, err := kinkeypair.Parse(val)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse seed")
	}

	full, ok := kp.(*kinkeypair.Full)
	if !ok {
		return nil, errors.New("seed was invalid")
	}

	return full, nil
}
