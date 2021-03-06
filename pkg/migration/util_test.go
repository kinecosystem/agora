package migration

import (
	"context"
	"crypto/ed25519"
	"sync"
	"testing"

	"github.com/kinecosystem/agora-common/headers"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/go/clients/horizon"
	"github.com/kinecosystem/go/strkey"
	"github.com/mr-tron/base58"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/testutil"
	"github.com/kinecosystem/agora/pkg/version"
)

type mockMigrator struct {
	sync.Mutex
	mock.Mock
}

func (m *mockMigrator) InitiateMigration(ctx context.Context, account ed25519.PublicKey, ignoreBalance bool, commitment solana.Commitment) error {
	m.Lock()
	defer m.Unlock()

	args := m.Called(ctx, account, ignoreBalance, commitment)
	return args.Error(0)
}

func (m *mockMigrator) GetMigrationAccount(ctx context.Context, account ed25519.PublicKey) (ed25519.PublicKey, error) {
	m.Lock()
	defer m.Unlock()

	args := m.Called(ctx, account)
	return args.Get(0).(ed25519.PublicKey), args.Error(1)
}

func TestMigrateBatch(t *testing.T) {
	ctx, err := headers.ContextWithHeaders(context.Background())
	require.NoError(t, err)
	require.NoError(t, headers.SetASCIIHeader(ctx, version.DesiredKinVersionHeader, "4"))

	accounts := testutil.GenerateSolanaKeys(t, 10)
	m := &mockMigrator{}

	var mu sync.Mutex
	callCount := make(map[string]int)

	m.On("InitiateMigration", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		mu.Lock()
		callCount[string(args.Get(1).(ed25519.PublicKey))]++
		mu.Unlock()
	})

	assert.NoError(t, MigrateBatch(ctx, m, accounts...))
	assert.Equal(t, len(callCount), 10)
	for _, v := range callCount {
		assert.Equal(t, 1, v)
	}
}

func TestMigrateTransferAccounts(t *testing.T) {
	ctx, err := headers.ContextWithHeaders(context.Background())
	require.NoError(t, err)
	require.NoError(t, headers.SetASCIIHeader(ctx, version.DesiredKinVersionHeader, "4"))

	accounts := testutil.GenerateSolanaKeys(t, 6)

	sender1, err := strkey.Encode(strkey.VersionByteAccountID, accounts[0])
	require.NoError(t, err)
	sender2, err := strkey.Encode(strkey.VersionByteAccountID, accounts[3])
	require.NoError(t, err)

	hc := &horizon.MockClient{}
	hc.On("LoadAccount", sender1).Return(*testutil.GenerateHorizonAccount("", "100", "1"), nil)
	hc.On("LoadAccount", sender2).Return(*testutil.GenerateHorizonAccount("", "0", "1"), nil)

	pairs := [][]ed25519.PublicKey{
		{accounts[0], accounts[1]},
		{accounts[0], accounts[2]},
		{accounts[3], accounts[4]},
		{accounts[3], accounts[5]},
	}
	m := &mockMigrator{}

	var mu sync.Mutex
	ignoreBalCallCount := 0
	normalCallCount := 0

	callCount := make(map[string]int)
	m.On("InitiateMigration", mock.Anything, mock.Anything, true, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		mu.Lock()
		callCount[string(args.Get(1).(ed25519.PublicKey))]++
		ignoreBalCallCount += 1
		mu.Unlock()
	})
	m.On("InitiateMigration", mock.Anything, mock.Anything, false, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		mu.Lock()
		callCount[string(args.Get(1).(ed25519.PublicKey))]++
		normalCallCount += 1
		mu.Unlock()
	})

	assert.NoError(t, MigrateTransferAccounts(ctx, hc, m, pairs...))
	assert.Equal(t, len(callCount), 5) // accounts[3] doesn't get migrated
	for _, v := range callCount {
		assert.Equal(t, 1, v)
	}
	assert.Equal(t, ignoreBalCallCount, 3)
	assert.Equal(t, normalCallCount, 2)
}

func TestMigrate_Error(t *testing.T) {
	ctx, err := headers.ContextWithHeaders(context.Background())
	require.NoError(t, err)
	require.NoError(t, headers.SetASCIIHeader(ctx, version.DesiredKinVersionHeader, "4"))

	accounts := testutil.GenerateSolanaKeys(t, 10)
	m := &mockMigrator{}

	m.On("InitiateMigration", mock.Anything, accounts[1], mock.Anything, mock.Anything).Return(errors.New("yikes"))
	m.On("InitiateMigration", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	assert.Error(t, MigrateBatch(ctx, m, accounts...))
}

func TestBlockhashEquality(t *testing.T) {
	var bh solana.Blockhash

	// Ensure this type of comparison behaves how we expect it.
	if bh != (solana.Blockhash{}) {
		assert.Fail(t, "should be equal")
	}

	bh[0] = 1

	if bh == (solana.Blockhash{}) {
		assert.Fail(t, "should not be equal")
	}
}

func TestDerivation(t *testing.T) {
	raw, err := base58.Decode("56TFPGGNL97wWLA9iiesmcbt9WRcMGmEFUbUqyXKPtaj")
	require.NoError(t, err)

	var generated []ed25519.PublicKey
	for i, s := range []string{
		"",
		"testmigrationkey1",
		"testmigrationkey2",
		"testmigrationkey3",
		"testmigrationkey4",
	} {
		pub, _, err := DeriveMigrationAccount(raw, []byte(s))
		require.NoError(t, err)
		pub2, _, err := DeriveMigrationAccount(raw, []byte(s))
		require.NoError(t, err)

		assert.Equal(t, pub, pub2)

		generated = append(generated, pub)
		if i > 0 {
			assert.NotEqual(t, pub, generated[i-1])
		}

	}
}
