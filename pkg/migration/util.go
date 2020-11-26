package migration

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"time"

	"github.com/kinecosystem/agora-common/headers"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kinecosystem/agora/pkg/version"
)

// MigrateBatch migrates a set of accounts in parallel.
//
// The function will deduplicate accounts, so callers need not be
// concerned.
func MigrateBatch(ctx context.Context, m Migrator, accounts ...ed25519.PublicKey) error {
	shouldMigrate, err := HasMigrationHeader(ctx)
	if !shouldMigrate {
		return err
	}

	accountSet := make(map[string]struct{})
	for _, a := range accounts {
		accountSet[string(a)] = struct{}{}
	}

	// todo: might need to bound the number of concurrent migrations
	g, _ := errgroup.WithContext(ctx)
	for account := range accountSet {
		a := account
		g.Go(func() error {
			return m.InitiateMigration(ctx, ed25519.PublicKey(a), solana.CommitmentSingle)
		})
	}

	return g.Wait()
}

// DeriveMigrationAccount derives a migration account address.
func DeriveMigrationAccount(source ed25519.PublicKey, secret []byte) (ed25519.PublicKey, ed25519.PrivateKey, error) {
	h := hmac.New(sha256.New, secret)
	_, err := h.Write(source)
	if err != nil {
		return nil, nil, err
	}
	return ed25519.GenerateKey(bytes.NewBuffer(h.Sum(nil)))
}

// HasMigrationHeader indicates whether or not the provided context
// contains a header indicating a migration should occur.
func HasMigrationHeader(ctx context.Context) (bool, error) {
	desired, err := version.GetCtxDesiredVersion(ctx)
	if err == nil {
		if desired >= 4 {
			return true, nil
		}
	}

	val, err := headers.GetASCIIHeaderByName(ctx, "enable-migration-hooks")
	if err != nil {
		return false, errors.Wrap(err, "failed to get migration header")
	}

	if len(val) == 0 {
		return false, nil
	}

	return val == "true", nil
}

// MarkComplete is a utility function to mark the state for an account as complete.
func MarkComplete(ctx context.Context, store Store, account ed25519.PublicKey, prev State) error {
	completedState := State{
		Status:       StatusComplete,
		LastModified: time.Now(),
	}

	return store.Update(ctx, account, prev, completedState)
}
