package specstate

import (
	"context"
	"crypto/ed25519"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v4"
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v4"

	"github.com/kinecosystem/agora/pkg/account/info"
	infomemory "github.com/kinecosystem/agora/pkg/account/info/memory"
	"github.com/kinecosystem/agora/pkg/testutil"
)

type testEnv struct {
	tokenAccount ed25519.PublicKey
	sc           *solana.MockClient
	infoCache    info.Cache
	loader       *Loader
}

type mockInfoCache struct {
	mu sync.Mutex
	mock.Mock
}

// Put puts a list of account info associated with the given owner.
func (m *mockInfoCache) Put(ctx context.Context, info *accountpb.AccountInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(ctx, info)
	return args.Error(0)
}

// Get gets an account's info, if it exists in the infoCache.
//
// ErrAccountInfoNotFound is returned if no account info was found for the provided key.
func (m *mockInfoCache) Get(ctx context.Context, key ed25519.PublicKey) (*accountpb.AccountInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(ctx, key)
	return args.Get(0).(*accountpb.AccountInfo), args.Error(1)
}

// Delete a cached entry.
func (m *mockInfoCache) Del(ctx context.Context, key ed25519.PublicKey) (ok bool, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(ctx, key)
	return args.Bool(0), args.Error(0)
}

func setupEnv(t *testing.T) (env *testEnv) {
	var err error
	env = &testEnv{}
	env.infoCache, err = infomemory.NewCache(5*time.Second, 5*time.Second, 1000)
	require.NoError(t, err)
	env.sc = solana.NewMockClient()
	env.tokenAccount = testutil.GenerateSolanaKeypair(t).Public().(ed25519.PublicKey)
	env.loader = NewSpeculativeLoader(token.NewClient(env.sc, env.tokenAccount), env.infoCache)
	return env
}

func TestLoad(t *testing.T) {
	env := setupEnv(t)

	accounts := testutil.GenerateSolanaKeys(t, 8)

	// Existing valid accounts: 0 -> 2
	for i := 0; i < 2; i++ {
		tokenInfo := token.Account{
			Mint:   env.tokenAccount,
			Owner:  accounts[i],
			Amount: 10 + uint64(i),
		}
		accountInfo := solana.AccountInfo{
			Data:  tokenInfo.Marshal(),
			Owner: token.ProgramKey,
		}

		env.sc.On("GetAccountInfo", accounts[i], mock.Anything).Return(accountInfo, nil)
	}

	// Existing, but wrong tokenAccount, accounts, 2-> 4
	for i := 2; i < 4; i++ {
		tokenInfo := token.Account{
			Mint:   accounts[i],
			Owner:  accounts[i],
			Amount: 10 + uint64(i),
		}
		accountInfo := solana.AccountInfo{
			Data:  tokenInfo.Marshal(),
			Owner: token.ProgramKey,
		}

		env.sc.On("GetAccountInfo", accounts[i], mock.Anything).Return(accountInfo, nil)
	}

	// No data at all for 4 -> 6
	for i := 4; i < 6; i++ {
		env.sc.On("GetAccountInfo", accounts[i], mock.Anything).Return(solana.AccountInfo{}, solana.ErrNoAccountInfo)
	}

	// No chain data at all for 6 -> 8, but infoCache data
	for i := 6; i < len(accounts); i++ {
		env.sc.On("GetAccountInfo", accounts[i], mock.Anything).Return(solana.AccountInfo{}, solana.ErrNoAccountInfo)
		assert.NoError(t, env.infoCache.Put(context.Background(), &accountpb.AccountInfo{
			AccountId: &commonpb.SolanaAccountId{
				Value: accounts[i],
			},
			Balance: int64(10 + i),
		}))
	}

	transferStates := make(map[string]int64)
	for i := 0; i < len(accounts); i++ {
		// We do this assignment formula such that
		// account 0 is negative (-1), and account 1 is positive,
		// ensuring we handle math correctly.
		transferStates[string(accounts[i])] = -1 + 2*int64(i)
	}

	newStates := env.loader.Load(context.Background(), transferStates, solana.CommitmentRecent)
	assert.Equal(t, 4, len(newStates))

	newState, ok := newStates[string(accounts[0])]
	assert.True(t, ok)
	assert.EqualValues(t, 10-1, newState)

	newState, ok = newStates[string(accounts[1])]
	assert.True(t, ok)
	assert.EqualValues(t, 11+1, newState)

	for i := 2; i < 6; i++ {
		_, ok = newStates[string(accounts[i])]
		assert.False(t, ok)
	}

	newState, ok = newStates[string(accounts[6])]
	assert.True(t, ok)
	assert.EqualValues(t, (10+6)+(-1)+2*6, newState)

	newState, ok = newStates[string(accounts[7])]
	assert.True(t, ok)
	assert.EqualValues(t, (10+7)+(-1)+2*7, newState)
}

func TestWrite(t *testing.T) {
	env := setupEnv(t)

	// super hack
	infoCache := &mockInfoCache{}
	env.loader.infoCache = infoCache

	accounts := testutil.GenerateSolanaKeys(t, 3)
	infoCache.On("Put", mock.Anything, mock.Anything).Return(errors.New("err")).Times(3)

	writes := map[string]int64{
		string(accounts[0]): 1,
		string(accounts[1]): 2,
		string(accounts[2]): 3,
	}

	// note: we can't actually test partial failures because proto messages
	// intentionally have features that break equality operators. Therefore, we
	// can only ensure that a failure doesn't prevent the others from trying
	// (using Times(3)), and nothing crashes
	env.loader.Write(context.Background(), writes)

	// AssertExpectations doesn't actually...work.
	assert.Equal(t, 3, len(infoCache.Calls))
}
