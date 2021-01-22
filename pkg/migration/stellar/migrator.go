package stellar

import (
	"context"
	"crypto/ed25519"
	"sync/atomic"
	"time"

	"github.com/kinecosystem/agora-common/kin/version"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/system"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/mr-tron/base58/base58"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/kinecosystem/agora/pkg/migration"
	"github.com/kinecosystem/agora/pkg/rate"
	"github.com/kinecosystem/agora/pkg/solanautil"
)

type accountInfo struct {
	account             ed25519.PublicKey
	migrationAccount    ed25519.PublicKey
	migrationAccountKey ed25519.PrivateKey
	owner               ed25519.PublicKey
	balance             uint64
}

type migrator struct {
	log     *logrus.Entry
	version version.KinVersion
	sc      solana.Client
	tc      *token.Client
	ml      migration.Loader
	limiter rate.Limiter
	store   migration.Store

	migrationSecret []byte

	subsidizer    ed25519.PublicKey
	subsidizerKey ed25519.PrivateKey
	source        ed25519.PublicKey
	sourceKey     ed25519.PrivateKey

	// Guarded by sync/atomc
	lamportSize uint64
}

func New(
	store migration.Store,
	version version.KinVersion,
	sc solana.Client,
	sl migration.Loader,
	limiter rate.Limiter,
	tokenAccount ed25519.PublicKey,
	subsidizer ed25519.PrivateKey,
	source ed25519.PublicKey,
	sourceKey ed25519.PrivateKey,
	migrationSecret []byte,
) migration.Migrator {
	return &migrator{
		log:             logrus.StandardLogger().WithField("type", "migration/stellar"),
		version:         version,
		sc:              sc,
		tc:              token.NewClient(sc, tokenAccount),
		ml:              sl,
		limiter:         limiter,
		store:           store,
		subsidizer:      subsidizer.Public().(ed25519.PublicKey),
		subsidizerKey:   subsidizer,
		source:          source,
		sourceKey:       sourceKey,
		migrationSecret: migrationSecret,
	}
}

func (m *migrator) InitiateMigration(ctx context.Context, account ed25519.PublicKey, ignoreBalance bool, commitment solana.Commitment) error {
	log := m.log.WithFields(logrus.Fields{
		"account":        base58.Encode(account),
		"ignore_balance": ignoreBalance,
		"commitment":     commitment,
	})

	migrationAccount, migrationAccountKey, err := migration.DeriveMigrationAccount(account, m.migrationSecret)
	if err != nil {
		return err
	}

	migrationCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	//
	// Check migration state store.
	//
	status, _, err := m.store.Get(migrationCtx, account)
	if err != nil {
		return err
	}
	switch status.Status {
	case migration.StatusComplete:
		return nil
	case migration.StatusInProgress:
		return m.recover(migrationCtx, account, migrationAccount, ignoreBalance, commitment)
	}

	//
	// Load necessary account information, and double check whether or not we should migrate.
	//
	info, err := m.loadAccount(migrationCtx, account, migrationAccountKey)
	switch err {
	case nil:
	case migration.ErrBurned, migration.ErrMultisig, migration.ErrNotFound:
		return err
	default:
		return errors.Wrap(err, "failed to load account info")
	}

	if !ignoreBalance && info.balance == 0 {
		return nil
	}

	// We check the rate limiter here instead of up there since we don't want to
	// rate limit accounts that don't need migrating.
	allowed, err := m.limiter.Allow("kin3_migration")
	if err != nil {
		return errors.Wrap(err, "failed to check migration rate limit")
	}
	if !allowed {
		migration.MigrationRateLimitedCounter.WithLabelValues(m.version.String()).Inc()
		return errors.New("rate limited")
	}

	migration.MigrationAllowedCounter.WithLabelValues(m.version.String()).Inc()

	if err := m.migrateAccount(migrationCtx, info, commitment); err != nil {
		migration.OnDemandFailureCounterVec.WithLabelValues(m.version.String()).Inc()
		log.WithError(err).Warn("failed to migrate account")
		return err
	}

	migration.OnDemandSuccessCounterVec.WithLabelValues(m.version.String()).Inc()
	return nil
}

func (m *migrator) GetMigrationAccounts(ctx context.Context, account ed25519.PublicKey) ([]ed25519.PublicKey, error) {
	migrationAccount, migrationAccountKey, err := migration.DeriveMigrationAccount(account, m.migrationSecret)
	if err != nil {
		return nil, err
	}

	_, err = m.loadAccount(ctx, account, migrationAccountKey)
	switch err {
	case nil:
	case migration.ErrBurned, migration.ErrMultisig, migration.ErrNotFound:
		return nil, err
	default:
		return nil, errors.Wrap(err, "failed to load account info")
	}

	return []ed25519.PublicKey{migrationAccount}, nil
}

func (m *migrator) migrateAccount(ctx context.Context, info accountInfo, commitment solana.Commitment) (err error) {
	log := m.log.WithField("method", "createMigrationAccount")

	lamports := atomic.LoadUint64(&m.lamportSize)
	if lamports == 0 {
		lamports, err = m.sc.GetMinimumBalanceForRentExemption(token.AccountSize)
		if err != nil {
			return errors.Wrap(err, "failed to get lamports")
		}

		atomic.StoreUint64(&m.lamportSize, lamports)
	}

	// Note: we rely on the solana client for caching
	bh, err := m.sc.GetRecentBlockhash()
	if err != nil {
		return errors.Wrap(err, "failed to get block hash")
	}

	// todo: support multisig (which involves adding more information in here)
	txn := solana.NewTransaction(
		m.subsidizer,
		system.CreateAccount(
			m.subsidizer,
			info.migrationAccount,
			token.ProgramKey,
			lamports,
			token.AccountSize,
		),
		token.InitializeAccount(
			info.migrationAccount,
			m.tc.Token(),
			info.migrationAccount,
		),
		token.SetAuthority(
			info.migrationAccount,
			info.migrationAccount,
			m.subsidizer,
			token.AuthorityTypeCloseAccount,
		),
		token.SetAuthority(
			info.migrationAccount,
			info.migrationAccount,
			info.owner,
			token.AuthorityTypeAccountHolder,
		),
		token.Transfer(
			m.source,
			info.migrationAccount,
			m.sourceKey.Public().(ed25519.PublicKey),
			info.balance,
		),
	)
	txn.SetBlockhash(bh)
	if err := txn.Sign(m.subsidizerKey, m.sourceKey, info.migrationAccountKey); err != nil {
		return errors.Wrap(err, "failed to sign migration transaction")
	}

	// Attempt to set the state as in progress.
	//
	// Note: It's important we attempt this before submission to reduce double submits.
	//       However, if we fail to do this, it doesn't reduce safety.
	state := migration.State{
		Status:       migration.StatusInProgress,
		Signature:    txn.Signatures[0],
		LastModified: time.Now(),
	}
	if err = m.store.Update(ctx, info.account, migration.ZeroState, state); err != nil {
		return errors.Wrap(err, "failed to mark migration as in progress")
	}

	_, stat, err := m.sc.SubmitTransaction(txn, commitment)
	if err != nil {
		if err == solana.ErrSignatureNotFound || solanautil.IsDuplicateSignature(err) {
			if _, tcErr := m.tc.GetAccount(info.migrationAccount, commitment); tcErr == nil {
				if commitment == solana.CommitmentMax || commitment == solana.CommitmentRoot {
					return errors.Wrap(migration.MarkComplete(ctx, m.store, info.account, state), "failed to mark migration as complete")
				}
			}
		}

		// We attempt to reset the state in the store for performance, not safety.
		// If we don't clear the state, the next attempt will query the signature,
		// which does not exist. Querying signatures that don't exist take a long
		// time.
		if stateErr := m.store.Update(ctx, info.account, state, migration.ZeroState); stateErr != nil {
			log.WithError(stateErr).Warn("failed to reset migration state")
		}

		return errors.Wrapf(err, "failed to submit transaction: %s", base58.Encode(txn.Signature()))
	}
	if stat.ErrorResult != nil {
		if !solanautil.IsAccountAlreadyExistsError(stat.ErrorResult) && !solanautil.IsDuplicateSignature(stat.ErrorResult) {
			return errors.Wrap(stat.ErrorResult, "submit transaction failed")
		}

		if commitment == solana.CommitmentMax {
			if _, tcErr := m.tc.GetAccount(info.migrationAccount, commitment); tcErr == nil {
				return errors.Wrap(migration.MarkComplete(ctx, m.store, info.account, state), "failed to mark migration as complete")
			}
		}
	}

	// If the transaction was finalized, we can mark the account as migrated.
	//
	// If not, we simply return a nil error, as SubmitTransaction() blocks until
	// the specified commitment has been met.
	if stat.Finalized() {
		return errors.Wrap(migration.MarkComplete(ctx, m.store, info.account, state), "failed to mark migration as complete")
	}

	return nil
}

func (m *migrator) recover(ctx context.Context, account, migrationAccount ed25519.PublicKey, ignoreBalance bool, commitment solana.Commitment) error {
	log := m.log.WithField("method", "recover")

	log.Trace("Recovering migration status")

	state, _, err := m.store.Get(context.Background(), account)
	if err != nil {
		return errors.Wrap(err, "failed to get status")
	}

	switch state.Status {
	case migration.StatusComplete:
		return nil
	case migration.StatusInProgress:
		if _, err := m.tc.GetAccount(migrationAccount, commitment); err == nil {
			if commitment == solana.CommitmentMax {
				return migration.MarkComplete(ctx, m.store, account, state)
			}

			return nil
		}

		// This is important for to circuit break the cycle between recover() and initiate().
		//
		// If we believe our status was InProgress, but we don't have any record of an account,
		// then we clear out our in progress status.
		if err := m.store.Update(ctx, account, state, migration.ZeroState); err != nil {
			return errors.Wrap(err, "failed to reset migration state")
		}

		fallthrough
	case migration.StatusNone:
		return m.InitiateMigration(ctx, account, ignoreBalance, commitment)
	}

	return errors.Errorf("unhandled state status: %v", state.Status)
}

func (m *migrator) loadAccount(ctx context.Context, account ed25519.PublicKey, migrationKey ed25519.PrivateKey) (info accountInfo, err error) {
	info.account = account
	info.migrationAccount = migrationKey.Public().(ed25519.PublicKey)
	info.migrationAccountKey = migrationKey

	info.owner, info.balance, err = m.ml.LoadAccount(account)
	return info, err
}
