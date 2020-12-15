package migration

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"net/http"
	"time"

	"github.com/kinecosystem/agora-common/headers"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/go/amount"
	"github.com/kinecosystem/go/clients/horizon"
	"github.com/kinecosystem/go/strkey"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kinecosystem/agora/pkg/version"
)

// MigrateBatch migrates a set of accounts in parallel.
//
// The function will deduplicate accounts, so callers need not be
// concerned.
func MigrateBatch(ctx context.Context, m Migrator, accounts ...ed25519.PublicKey) error {
	accountSet := make(map[string]struct{})
	for _, a := range accounts {
		accountSet[string(a)] = struct{}{}
	}

	// todo: might need to bound the number of concurrent migrations
	g, _ := errgroup.WithContext(ctx)
	for account := range accountSet {
		a := account
		g.Go(func() error {
			return m.InitiateMigration(ctx, ed25519.PublicKey(a), false, solana.CommitmentSingle)
		})
	}

	return g.Wait()
}

// Migrate the accounts involved in a transaction in parallel.
//
// Ensures a migration for destinations occurs if a sender sending a transfer to it has a native balance.
func MigrateTransferAccounts(ctx context.Context, hc horizon.ClientInterface, m Migrator, transferAccountPairs ...[]ed25519.PublicKey) error {
	sendersToDests := make(map[string][]ed25519.PublicKey)
	for _, pair := range transferAccountPairs {
		if len(pair) != 2 {
			return errors.New("invalid number of accounts")
		}
		sendersToDests[string(pair[0])] = append(sendersToDests[string(pair[0])], pair[1])
	}

	normalAccounts := make(map[string]struct{})
	ignoreBalanceAccounts := make(map[string]struct{})
	for sender, dests := range sendersToDests {
		address, err := strkey.Encode(strkey.VersionByteAccountID, []byte(sender))
		if err != nil {
			return errors.Wrap(err, "failed to encode sender as stellar address")
		}

		stellarAccount, err := hc.LoadAccount(address)
		if err != nil {
			if hErr, ok := err.(*horizon.Error); ok {
				switch hErr.Problem.Status {
				case http.StatusNotFound:
					for _, dest := range dests {
						if _, ok := ignoreBalanceAccounts[string(dest)]; !ok {
							normalAccounts[string(dest)] = struct{}{}
						}
					}
				}
			}
			return errors.New("failed to load account")
		} else {
			strBalance, err := stellarAccount.GetNativeBalance()
			if err != nil {
				return errors.Wrap(err, "failed to get native balance")
			}
			balance, err := amount.ParseInt64(strBalance)
			if err != nil {
				return errors.Wrap(err, "failed to parse balance")
			}

			if balance > 0 {
				for _, a := range append([]ed25519.PublicKey{ed25519.PublicKey(sender)}, dests...) {
					delete(normalAccounts, string(a))
					ignoreBalanceAccounts[string(a)] = struct{}{}
				}
			} else {
				for _, a := range dests {
					if _, ok := ignoreBalanceAccounts[string(a)]; !ok {
						normalAccounts[string(a)] = struct{}{}
					}
				}
			}
		}
	}

	g, _ := errgroup.WithContext(ctx)
	for account := range ignoreBalanceAccounts {
		a := account
		g.Go(func() error {
			return m.InitiateMigration(ctx, ed25519.PublicKey(a), true, solana.CommitmentSingle)
		})
	}
	for account := range normalAccounts {
		a := account
		g.Go(func() error {
			return m.InitiateMigration(ctx, ed25519.PublicKey(a), false, solana.CommitmentSingle)
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

	err := store.Update(ctx, account, prev, completedState)
	if err != nil {
		markCompleteFailureCounter.Inc()
		return err
	}

	markCompleteSuccessCounter.Inc()
	return nil
}
