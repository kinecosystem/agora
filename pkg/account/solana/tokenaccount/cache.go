package tokenaccount

import (
	"bytes"
	"context"
	"crypto/ed25519"

	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	ErrTokenAccountsNotFound = errors.New("no token accounts found")
)

type Cache interface {
	// Put puts a list of token accounts associated with the given owner.
	Put(ctx context.Context, owner ed25519.PublicKey, tokenAccounts []ed25519.PublicKey) error

	// Get gets an owner's token accounts, if it exists in the cache.
	//
	// ErrTokenAccountsNotFound is returned if no token accounts were cached for the provided owner.
	Get(ctx context.Context, owner ed25519.PublicKey) ([]ed25519.PublicKey, error)

	// Delete removes any cached token accounts for an owner.
	//
	// Delete is idempotent.
	Delete(ctx context.Context, owner ed25519.PublicKey) error
}

type cacheUpdater struct {
	log   *logrus.Entry
	cache Cache
	mint  ed25519.PublicKey
}

func NewCacheUpdater(cache Cache, mint ed25519.PublicKey) (*cacheUpdater, error) {
	return &cacheUpdater{
		log:   logrus.StandardLogger().WithField("type", "account/solana/cache"),
		cache: cache,
		mint:  mint,
	}, nil
}

// OnTransaction implements transaction.Notifier.OnTransaction
func (t *cacheUpdater) OnTransaction(txn solana.BlockTransaction) {
	log := t.log.WithField("method", "OnTransaction")

	accounts := make(map[string]struct{})
	for i := range txn.Transaction.Message.Instructions {
		init, err := token.DecompileInitializeAccount(txn.Transaction.Message, i)
		if err == nil {
			if bytes.Equal(init.Mint, t.mint) {
				accounts[string(init.Owner)] = struct{}{}
			}
			continue
		} else if err != solana.ErrIncorrectInstruction && err != solana.ErrIncorrectProgram {
			log.WithError(err).Warn("failed to decompile initialize account instruction")
			continue
		}

		setAuth, err := token.DecompileSetAuthority(txn.Transaction.Message, i)
		if err == nil {
			if setAuth.Type == token.AuthorityTypeAccountHolder {
				accounts[string(setAuth.CurrentAuthority)] = struct{}{}
				accounts[string(setAuth.NewAuthority)] = struct{}{}
			}
			continue
		} else if err != solana.ErrIncorrectInstruction && err != solana.ErrIncorrectProgram {
			log.WithError(err).Warn("failed to decompile set authority instruction")
			continue
		}
	}

	// Remove any affected accounts
	for accountID := range accounts {
		err := t.cache.Delete(context.Background(), []byte(accountID))
		if err != nil {
			log.WithError(err).Warn("failed to delete owner from cache")
		}
	}
}
