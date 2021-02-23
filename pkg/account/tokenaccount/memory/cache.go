package memory

import (
	"context"
	"crypto/ed25519"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/mr-tron/base58"
	"github.com/pkg/errors"

	"github.com/kinecosystem/agora/pkg/account/tokenaccount"
)

type cache struct {
	cache *lru.Cache

	ttl time.Duration
}

type tokenAccountEntry struct {
	created  time.Time
	accounts []ed25519.PublicKey
}

func New(itemTTL time.Duration, maxSize int) (tokenaccount.Cache, error) {
	lruCache, err := lru.New(maxSize)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create token account cache")
	}

	return &cache{
		cache: lruCache,
		ttl:   itemTTL,
	}, nil
}

func (c *cache) Put(ctx context.Context, owner ed25519.PublicKey, tokenAccounts []ed25519.PublicKey) error {
	c.cache.Add(base58.Encode(owner), &tokenAccountEntry{
		created:  time.Now(),
		accounts: tokenAccounts,
	})
	return nil
}

func (c *cache) Get(ctx context.Context, owner ed25519.PublicKey) ([]ed25519.PublicKey, error) {
	key := base58.Encode(owner)
	cached, ok := c.cache.Get(key)
	if ok {
		entry := cached.(*tokenAccountEntry)
		if time.Since(entry.created) < c.ttl && len(entry.accounts) > 0 {
			return entry.accounts, nil
		}

		c.cache.Remove(key)
	}

	return nil, tokenaccount.ErrTokenAccountsNotFound
}

func (c *cache) Delete(ctx context.Context, owner ed25519.PublicKey) error {
	c.cache.Remove(base58.Encode(owner))
	return nil
}

func (c *cache) reset() {
	c.cache.Purge()
}
