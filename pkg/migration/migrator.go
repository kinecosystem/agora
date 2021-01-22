package migration

import (
	"context"
	"crypto/ed25519"

	"github.com/kinecosystem/agora-common/solana"
	"github.com/mr-tron/base58"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	ErrMultisig    = errors.New("multisig wallet")
	ErrNotFound    = errors.New("account not found")
	ErrBurned      = errors.New("account was burned")
	ErrRateLimited = errors.New("rate limited")
)

type Loader interface {
	LoadAccount(account ed25519.PublicKey) (owner ed25519.PublicKey, balance uint64, err error)
}

type noopLoader struct{}

func NewNoopLoader() Loader {
	return &noopLoader{}
}

func (l *noopLoader) LoadAccount(account ed25519.PublicKey) (owner ed25519.PublicKey, balance uint64, err error) {
	return nil, 0, ErrNotFound
}

type Migrator interface {
	// InitiateMigration initiates a migration for a given account.
	//
	// The commitment provided indicates the commitment that should be used for
	// transactions and queries before returning. It should be noted that any
	// commitment less than MAX will not mark a migration as completed.
	InitiateMigration(ctx context.Context, account ed25519.PublicKey, ignoreBalance bool, commitment solana.Commitment) error

	// GetMigrationAccounts returns the set of migration accounts for the public key, _only if_
	// a migration would occur for said account.
	//
	// That is, this only returns the derived (migration) accounts if the provided public
	// key exists on an older chain.
	GetMigrationAccounts(ctx context.Context, account ed25519.PublicKey) ([]ed25519.PublicKey, error)
}

type noopMigrator struct{}

func NewNoopMigrator() Migrator {
	return &noopMigrator{}
}

func (m *noopMigrator) InitiateMigration(context.Context, ed25519.PublicKey, bool, solana.Commitment) error {
	return nil
}

func (m *noopMigrator) GetMigrationAccounts(context.Context, ed25519.PublicKey) ([]ed25519.PublicKey, error) {
	return nil, nil
}

type contextAwareMigrator struct {
	base Migrator
}

func NewContextAwareMigrator(base Migrator) Migrator {
	return &contextAwareMigrator{base: base}
}

func (m *contextAwareMigrator) InitiateMigration(ctx context.Context, account ed25519.PublicKey, ignoreBalance bool, commitment solana.Commitment) error {
	initiateMigrationBeforeCounter.Inc()

	hasMigrationheader, err := HasMigrationHeader(ctx)
	if !hasMigrationheader {
		return err
	}

	initiateMigrationAfterCounter.Inc()
	return m.base.InitiateMigration(ctx, account, ignoreBalance, commitment)
}

func (m *contextAwareMigrator) GetMigrationAccounts(ctx context.Context, account ed25519.PublicKey) ([]ed25519.PublicKey, error) {
	return m.base.GetMigrationAccounts(ctx, account)
}

type dualChainMigrator struct {
	log  *logrus.Entry
	conf conf

	kin2 Migrator
	kin3 Migrator
}

func NewDualChainMigrator(kin2, kin3 Migrator, c ConfigProvider) Migrator {
	m := &dualChainMigrator{
		log:  logrus.StandardLogger().WithField("type", "migration/dual"),
		kin2: kin2,
		kin3: kin3,
	}
	c(&m.conf)
	return m
}

func (m *dualChainMigrator) InitiateMigration(ctx context.Context, account ed25519.PublicKey, ignoreBalance bool, commitment solana.Commitment) error {
	log := m.log.WithFields(logrus.Fields{
		"account": base58.Encode(account),
	})

	if !m.conf.kin2Enabled.Get(ctx) && !m.conf.kin3Enabled.Get(ctx) {
		log.Debug("No migrators enabled; ignoring")
		return nil
	}

	var errNotInitiated = errors.New("not initiated")

	var kin2Err, kin3Err error = errNotInitiated, errNotInitiated
	if m.conf.kin2Enabled.Get(ctx) {
		kin2Err = m.kin2.InitiateMigration(ctx, account, ignoreBalance, commitment)
	}
	if m.conf.kin3Enabled.Get(ctx) {
		kin3Err = m.kin3.InitiateMigration(ctx, account, ignoreBalance, commitment)
	}

	log = log.WithFields(logrus.Fields{
		"kin2_result": kin2Err,
		"kin3_result": kin3Err,
	})

	// If only one of the migrators is enabled, just forward the error.
	if kin2Err == errNotInitiated && kin3Err != errNotInitiated {
		log.WithField("effective_result", kin3Err).Debug("Migration initiated")
		return kin3Err
	}
	if kin3Err == errNotInitiated && kin2Err != errNotInitiated {
		log.WithField("effective_result", kin2Err).Debug("Migration initiated")
		return kin2Err
	}

	var effectiveErr error

	switch kin2Err {
	case nil:
		// A successful migration occurred for kin2. We only need
		// to return an error if there was _supposed_ to be a successful
		// migration on kin3, and it failed
		if kin3Err != ErrNotFound && kin3Err != ErrBurned {
			effectiveErr = kin3Err
		}
	case ErrNotFound, ErrBurned:
		// The account effectively doesn't exist on kin2, so just return
		// the kin3 result
		effectiveErr = kin3Err
	case ErrMultisig:
		effectiveErr = ErrMultisig
	default:
		effectiveErr = errors.Wrap(kin2Err, "failed to perform kin2 migration")
	}

	log.WithField("effective_result", effectiveErr).Debug("Migration initiated")

	return effectiveErr
}

func (m *dualChainMigrator) GetMigrationAccounts(ctx context.Context, account ed25519.PublicKey) (accounts []ed25519.PublicKey, err error) {
	// Note: the implementations of GetMigrationAccounts only return an account
	// in the case of no error (which is mostly what we want). As a result, we
	// can have the simplified logic below.
	if m.conf.kin2Enabled.Get(ctx) {
		kin2Accounts, err := m.kin2.GetMigrationAccounts(ctx, account)
		switch err {
		case nil, ErrBurned, ErrMultisig, ErrNotFound:
			// note: we don't return errors here since there may be
			// a useful account on kin3
		default:
			return nil, errors.Wrap(err, "failed to get kin2 migration accounts")
		}

		accounts = append(accounts, kin2Accounts...)
	}

	if m.conf.kin3Enabled.Get(ctx) {
		kin3Accounts, err := m.kin3.GetMigrationAccounts(ctx, account)
		switch err {
		case nil:
		case ErrBurned, ErrMultisig, ErrNotFound:
			// If we haven't retrieved any account from kin2, we can just
			// return the error. Otherwise the error here might be because
			// the account was only on kin2.
			if len(accounts) == 0 {
				return nil, err
			}
		default:
			return nil, errors.Wrap(err, "failed to get kin3 migration accounts")
		}
		accounts = append(accounts, kin3Accounts...)
	}

	return accounts, nil
}
