package kin2

import (
	"context"
	"crypto/ed25519"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/system"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/kinecosystem/go/amount"
	"github.com/kinecosystem/go/clients/horizon"
	"github.com/kinecosystem/go/strkey"
	"github.com/mr-tron/base58/base58"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/kinecosystem/agora/pkg/migration"
	"github.com/kinecosystem/agora/pkg/solanautil"
)

type accountInfo struct {
	account             ed25519.PublicKey
	migrationAccount    ed25519.PublicKey
	migrationAccountKey ed25519.PrivateKey
	owner               ed25519.PublicKey
	balance             uint64
}

type kin2Migrator struct {
	log        *logrus.Entry
	sc         solana.Client
	tc         *token.Client
	hc         horizon.ClientInterface
	kin2Issuer string
	store      migration.Store

	migrationSecret []byte

	subsidizer    ed25519.PublicKey
	subsidizerKey ed25519.PrivateKey
	source        ed25519.PublicKey
	sourceKey     ed25519.PrivateKey

	// Guarded by sync/atomc
	lamportSize uint64

	blockhashMu sync.RWMutex
	lastAccess  time.Time
	blockhash   solana.Blockhash
}

func New(
	store migration.Store,
	sc solana.Client,
	hc horizon.ClientInterface,
	kin2Issuer string,
	tokenAccount ed25519.PublicKey,
	subsidizer ed25519.PrivateKey,
	source ed25519.PublicKey,
	sourceKey ed25519.PrivateKey,
	migrationSecret []byte,
) migration.Migrator {
	return &kin2Migrator{
		log:             logrus.StandardLogger().WithField("type", "migration/kin2migrator"),
		sc:              sc,
		kin2Issuer:      kin2Issuer,
		tc:              token.NewClient(sc, tokenAccount),
		store:           store,
		hc:              hc,
		subsidizer:      subsidizer.Public().(ed25519.PublicKey),
		subsidizerKey:   subsidizer,
		source:          source,
		sourceKey:       sourceKey,
		migrationSecret: migrationSecret,
	}
}

func (m *kin2Migrator) InitiateMigration(ctx context.Context, account ed25519.PublicKey, commitment solana.Commitment) error {
	migrationAccount, migrationAccountKey, err := migration.DeriveMigrationAccount(account, m.migrationSecret)
	if err != nil {
		return err
	}

	//
	// Check migration state store.
	//
	status, err := m.store.Get(context.Background(), account)
	if err != nil {
		return err
	}
	switch status.Status {
	case migration.StatusComplete:
		return nil
	case migration.StatusInProgress:
		return m.recover(ctx, account, migrationAccount, commitment)
	}

	// WARNING: this currently ignores kin3 all together. this should be fixed to either merge balances into
	//          a single account.

	//
	// Load necessary account information, and double check whether or not we should migrate.
	//
	info, err := m.loadAccount(ctx, account, migrationAccountKey)
	switch err {
	case nil, migration.ErrNotFound:
		// todo(offline): we actually care about ErrNotFound, since it means
		//                we might be on a wrong config.
	case migration.ErrBurned, migration.ErrMultisig:
		return err
	default:
		return errors.Wrap(err, "failed to load account info")
	}

	if info.balance == 0 {
		return nil
	}

	return m.migrateAccount(ctx, info, commitment)
}

func (m *kin2Migrator) migrateAccount(ctx context.Context, info accountInfo, commitment solana.Commitment) (err error) {
	log := m.log.WithField("method", "createMigrationAccount")

	lamports := atomic.LoadUint64(&m.lamportSize)
	if lamports == 0 {
		lamports, err = m.sc.GetMinimumBalanceForRentExemption(token.AccountSize)
		if err != nil {
			return errors.Wrap(err, "failed to get lamports")
		}

		atomic.StoreUint64(&m.lamportSize, lamports)
	}

	var bh solana.Blockhash

	// To avoid having thrashing around a similar periodic interval, we
	// randomize when we refresh our block hash. This is mostly only a
	// concern when running a batch migrator with a _ton_ of goroutines.
	window := time.Duration(float64(20*time.Second) * (0.5 + rand.Float64()/2.0))

	m.blockhashMu.RLock()
	if m.blockhash == (solana.Blockhash{}) || time.Since(m.lastAccess) > window {
		m.blockhashMu.RUnlock()

		// We query outside of the exclusive zone. We _should_ be well within
		// the recent blockhash times (quoted from the devs at ~2 minutes), so
		// it's ok if someone else set a newer or older value.
		bh, err = m.sc.GetRecentBlockhash()
		if err != nil {
			return errors.Wrap(err, "failed to get block hash")
		}

		m.blockhashMu.Lock()
		m.blockhash = bh
		m.lastAccess = time.Now()
		m.blockhashMu.Unlock()
	} else {
		bh = m.blockhash
		m.blockhashMu.RUnlock()
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
		if !solanautil.IsAccountAlreadyExistsError(stat.ErrorResult) {
			return errors.Wrap(stat.ErrorResult, "submit transaction failed")
		}

		if commitment == solana.CommitmentMax {
			return errors.Wrap(migration.MarkComplete(ctx, m.store, info.account, state), "failed to mark migration as complete")
		}

	}

	// If the confirmations is nil, then the transaction was rooted, and therefore
	// considered irreversible. At this point we can mark the account as migrated.
	//
	// If not, we simply return a nil error, as SubmitTransaction() blocks until
	// the specified commitment has been met.
	if stat.Confirmations == nil {
		return errors.Wrap(migration.MarkComplete(ctx, m.store, info.account, state), "failed to mark migration as complete")
	}

	return nil
}

func (m *kin2Migrator) recover(ctx context.Context, account, migrationAccount ed25519.PublicKey, commitment solana.Commitment) error {
	log := m.log.WithField("method", "recover")

	log.Trace("Recovering migration status")

	state, err := m.store.Get(context.Background(), account)
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
		return m.InitiateMigration(ctx, account, commitment)
	}

	return errors.Errorf("unhandled state status: %v", state.Status)
}

func (m *kin2Migrator) loadAccount(ctx context.Context, account ed25519.PublicKey, migrationKey ed25519.PrivateKey) (info accountInfo, err error) {
	info.account = account
	info.migrationAccount = migrationKey.Public().(ed25519.PublicKey)
	info.migrationAccountKey = migrationKey

	address, err := strkey.Encode(strkey.VersionByteAccountID, account)
	if err != nil {
		return info, errors.Wrap(err, "failed to encode account as stellar address")
	}

	stellarAccount, err := m.hc.LoadAccount(address)
	if err != nil {
		if hErr, ok := err.(*horizon.Error); ok {
			switch hErr.Problem.Status {
			case http.StatusNotFound:
				return info, migration.ErrNotFound
			}
		}

		return info, errors.Wrap(err, "failed to check kin2 account status")
	}
	strBalance := stellarAccount.GetCreditBalance(kin.KinAssetCode, m.kin2Issuer)
	balance, err := amount.ParseInt64(strBalance)
	if err != nil {
		return info, errors.Wrap(err, "failed to parse balance")
	}
	if balance < 0 {
		return info, errors.Errorf("cannot migrate negative balance: %d", balance)
	}

	info.balance = uint64(balance)

	nonZeroSigners := make([]horizon.Signer, 0, len(stellarAccount.Signers))
	for _, s := range stellarAccount.Signers {
		if s.Weight > 0 {
			nonZeroSigners = append(nonZeroSigners, s)
		}
	}

	if len(nonZeroSigners) > 1 {
		return info, migration.ErrMultisig
	} else if len(nonZeroSigners) == 0 {
		return info, migration.ErrBurned
	}

	info.owner, err = strkey.Decode(strkey.VersionByteAccountID, nonZeroSigners[0].Key)
	if err != nil {
		return info, errors.Wrap(err, "failed to decode owner key")
	}

	return info, nil
}
