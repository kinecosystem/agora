package tokenaccount

import (
	"context"
	"crypto/ed25519"
	"testing"

	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/memo"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/testutil"
)

func TestCacheUpdater_OnTransaction(t *testing.T) {
	cache := newTestCache()
	mint := testutil.GenerateSolanaKeypair(t).Public().(ed25519.PublicKey)
	invalidator, err := NewCacheUpdater(cache, mint)
	require.NoError(t, err)

	subsidizer := testutil.GenerateSolanaKeypair(t)
	owners := testutil.GenerateSolanaKeys(t, 3)
	tokenAccounts := testutil.GenerateSolanaKeys(t, 3)

	require.NoError(t, cache.Put(context.Background(), owners[0], tokenAccounts))
	require.NoError(t, cache.Put(context.Background(), owners[1], tokenAccounts))
	require.NoError(t, cache.Put(context.Background(), owners[2], tokenAccounts))

	// Invalidate owner 0 with an InitializeAccount instruction.
	otherMint := testutil.GenerateSolanaKeypair(t).Public().(ed25519.PublicKey)
	txn := solana.NewTransaction(
		subsidizer.Public().(ed25519.PublicKey),
		memo.Instruction("test"),                                         // should be ignored (wrong program)
		token.Transfer(tokenAccounts[0], tokenAccounts[1], owners[0], 1), // should be ignored (wrong instruction)
		token.InitializeAccount(tokenAccounts[0], mint, owners[0]),       // invalidates owner 0
		token.InitializeAccount(tokenAccounts[0], otherMint, owners[1]),  // should be ignored (wrong mint)
	)
	require.NoError(t, txn.Sign(subsidizer))

	cached, err := cache.Get(context.Background(), owners[0])
	assert.NoError(t, err)
	assert.Equal(t, tokenAccounts, cached)

	invalidator.OnTransaction(solana.BlockTransaction{Transaction: txn})
	cached, err = cache.Get(context.Background(), owners[0])
	assert.Equal(t, ErrTokenAccountsNotFound, err)
	assert.Nil(t, cached)

	// Assert owner 1 & 2 still exists
	for _, owner := range owners[1:] {
		cached, err := cache.Get(context.Background(), owner)
		require.NoError(t, err)
		assert.NotNil(t, cached)
	}

	// Re-add owner 0
	require.NoError(t, cache.Put(context.Background(), owners[0], tokenAccounts))

	// Invalidate owner 1 and 2 with SetAuthority instructions
	txn = solana.NewTransaction(
		subsidizer.Public().(ed25519.PublicKey),
		token.SetAuthority(tokenAccounts[0], owners[1], mint, token.AuthorityTypeAccountHolder), // invalidates owner 1
		token.SetAuthority(tokenAccounts[0], mint, owners[2], token.AuthorityTypeAccountHolder), // invalidates owner 2
		token.SetAuthority(tokenAccounts[0], owners[0], mint, token.AuthorityTypeCloseAccount),  // should be ignored (wrong auth type)
	)
	require.NoError(t, txn.Sign(subsidizer))

	invalidator.OnTransaction(solana.BlockTransaction{Transaction: txn})

	// Assert owner 1 & 2 are invalidated
	for _, owner := range owners[1:3] {
		cached, err := cache.Get(context.Background(), owner)
		assert.Equal(t, ErrTokenAccountsNotFound, err)
		assert.Nil(t, cached)
	}

	// Assert owner 0 still exists
	cached, err = cache.Get(context.Background(), owners[0])
	require.NoError(t, err)
	assert.NotNil(t, cached)

	// Re-add owners 1 and 2
	require.NoError(t, cache.Put(context.Background(), owners[1], tokenAccounts))
	require.NoError(t, cache.Put(context.Background(), owners[2], tokenAccounts))

	// Invalidate owner 1 with CloseAccount
	txn = solana.NewTransaction(
		subsidizer.Public().(ed25519.PublicKey),
		token.CloseAccount(tokenAccounts[0], tokenAccounts[1], owners[0]),
	)
	require.NoError(t, txn.Sign(subsidizer))

	invalidator.OnTransaction(solana.BlockTransaction{Transaction: txn})

	cached, err = cache.Get(context.Background(), owners[0])
	assert.Equal(t, ErrTokenAccountsNotFound, err)
	assert.Nil(t, cached)

	// Re-add owners 1 and 2
	require.NoError(t, cache.Put(context.Background(), owners[1], tokenAccounts))
	require.NoError(t, cache.Put(context.Background(), owners[2], tokenAccounts))

	// Invalidate owner 1 with CreateAssoc
	createAssoc, _, err := token.CreateAssociatedTokenAccount(subsidizer.Public().(ed25519.PublicKey), owners[1], mint)
	require.NoError(t, err)

	txn = solana.NewTransaction(
		subsidizer.Public().(ed25519.PublicKey),
		createAssoc,
	)
	require.NoError(t, txn.Sign(subsidizer))

	invalidator.OnTransaction(solana.BlockTransaction{Transaction: txn})

	cached, err = cache.Get(context.Background(), owners[1])
	assert.Equal(t, ErrTokenAccountsNotFound, err)
	assert.Nil(t, cached)
}

type testCache struct {
	cache map[string][]ed25519.PublicKey
}

func (t *testCache) Put(ctx context.Context, owner ed25519.PublicKey, tokenAccounts []ed25519.PublicKey) error {
	t.cache[base58.Encode(owner)] = tokenAccounts
	return nil
}

func (t *testCache) Get(ctx context.Context, owner ed25519.PublicKey) ([]ed25519.PublicKey, error) {
	cached, ok := t.cache[base58.Encode(owner)]
	if !ok {
		return nil, ErrTokenAccountsNotFound
	}
	return cached, nil
}

func (t *testCache) Delete(ctx context.Context, owner ed25519.PublicKey) error {
	delete(t.cache, base58.Encode(owner))
	return nil
}

func newTestCache() Cache {
	return &testCache{cache: make(map[string][]ed25519.PublicKey)}
}
