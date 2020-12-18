package memory

import (
	"context"
	"crypto/ed25519"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/pkg/errors"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v4"

	"github.com/kinecosystem/agora/pkg/account/solana/accountinfo"
)

type cache struct {
	cache *lru.Cache

	ttl         time.Duration
	negativeTTL time.Duration
}

type entry struct {
	created time.Time
	info    *accountpb.AccountInfo
}

func New(itemTTL, negativeTTL time.Duration, maxSize int) (accountinfo.Cache, error) {
	lruCache, err := lru.New(maxSize)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create account info cache")
	}

	return &cache{
		cache:       lruCache,
		ttl:         itemTTL,
		negativeTTL: negativeTTL,
	}, nil
}

func (c *cache) Put(ctx context.Context, info *accountpb.AccountInfo) error {
	if info == nil || len(info.AccountId.Value) != ed25519.PublicKeySize {
		return errors.New("info must not be nil and account ID must be a valid ed25519 public key")
	}

	c.cache.Add(string(info.AccountId.Value), &entry{
		created: time.Now(),
		info:    info,
	})
	return nil
}

func (c *cache) Get(ctx context.Context, key ed25519.PublicKey) (*accountpb.AccountInfo, error) {
	cached, ok := c.cache.Get(string(key))
	if ok {
		entry := cached.(*entry)
		ttl := c.ttl
		if entry.info.Balance < 0 {
			ttl = c.negativeTTL
		}

		if time.Since(entry.created) < ttl && entry.info != nil {
			return entry.info, nil
		}

		c.cache.Remove(string(key))
	}

	return nil, accountinfo.ErrAccountInfoNotFound
}

func (c *cache) Del(ctx context.Context, key ed25519.PublicKey) (bool, error) {
	_, ok := c.cache.Get(string(key))
	if !ok {
		return false, nil
	}
	c.cache.Remove(string(key))
	return true, nil
}

func (c *cache) reset() {
	c.cache.Purge()
}
