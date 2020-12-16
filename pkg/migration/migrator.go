package migration

import (
	"context"
	"crypto/ed25519"

	"github.com/kinecosystem/agora-common/solana"
	"github.com/pkg/errors"
)

var (
	ErrMultisig    = errors.New("multisig wallet")
	ErrNotFound    = errors.New("account not found")
	ErrBurned      = errors.New("account was burned")
	ErrRateLimited = errors.New("rate limited")
)

type Migrator interface {
	// InitiateMigration initiates a migration for a given account.
	//
	// The commitment provided indicates the commitment that should be used for
	// transactions and queries before returning. It should be noted that any
	// commitment less than MAX will not mark a migration as completed.
	InitiateMigration(ctx context.Context, publicKey ed25519.PublicKey, ignoreBalance bool, commitment solana.Commitment) error
}

type noopMigrator struct{}

func NewNoopMigrator() Migrator {
	return &noopMigrator{}
}

func (m *noopMigrator) InitiateMigration(context.Context, ed25519.PublicKey, bool, solana.Commitment) error {
	return nil
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
