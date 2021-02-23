package tests

import (
	"context"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/agora-api/genproto/account/v4"
	"github.com/kinecosystem/agora-api/genproto/common/v4"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ed25519"

	"github.com/kinecosystem/agora/pkg/account/info"
	infocache "github.com/kinecosystem/agora/pkg/account/info/memory"
	tokencache "github.com/kinecosystem/agora/pkg/account/tokenaccount/memory"
	"github.com/kinecosystem/agora/pkg/testutil"
)

func TestLoader_Load(t *testing.T) {
	accounts := testutil.GenerateSolanaKeys(t, 4)
	owner := testutil.GenerateSolanaKeys(t, 1)[0]

	sc := solana.NewMockClient()
	tc := token.NewClient(sc, accounts[0])

	accountInfoCache, err := infocache.NewCache(time.Minute, time.Minute, 100)
	require.NoError(t, err)
	tokenCache, err := tokencache.New(time.Minute, 30)
	require.NoError(t, err)

	loader := info.NewLoader(tc, accountInfoCache, tokenCache)

	tokenInfo := token.Account{
		Mint:   accounts[0],
		Owner:  owner,
		Amount: 1,
	}
	otherTokenInfo := token.Account{
		Mint:   accounts[1],
		Owner:  accounts[2],
		Amount: 1,
	}
	accountInfos := []solana.AccountInfo{
		{
			Data:  tokenInfo.Marshal(),
			Owner: token.ProgramKey,
		},
		{
			Data:  otherTokenInfo.Marshal(),
			Owner: token.ProgramKey,
		},
	}

	sc.On("GetAccountInfo", accounts[1], mock.Anything).Return(solana.AccountInfo{}, solana.ErrNoAccountInfo)
	sc.On("GetAccountInfo", accounts[2], mock.Anything).Return(accountInfos[0], nil)
	sc.On("GetAccountInfo", accounts[3], mock.Anything).Return(accountInfos[1], nil)

	_, err = loader.Load(context.Background(), accounts[1], solana.CommitmentMax)
	assert.Equal(t, err, info.ErrAccountInfoNotFound)
	_, err = loader.Load(context.Background(), accounts[3], solana.CommitmentMax)
	assert.Equal(t, err, info.ErrAccountInfoNotFound)

	accountInfo, err := loader.Load(context.Background(), accounts[2], solana.CommitmentMax)
	assert.NoError(t, err)
	assert.EqualValues(t, accounts[2], accountInfo.AccountId.Value)
	assert.EqualValues(t, 1, accountInfo.Balance)

	// writeback
	sc.ExpectedCalls = nil

	// Negative caching
	_, err = loader.Load(context.Background(), accounts[1], solana.CommitmentMax)
	assert.Equal(t, err, info.ErrAccountInfoNotFound)

	// Positive caching
	accountInfo, err = loader.Load(context.Background(), accounts[2], solana.CommitmentMax)
	assert.NoError(t, err)
	assert.EqualValues(t, accounts[2], accountInfo.AccountId.Value)
	assert.EqualValues(t, 1, accountInfo.Balance)

	// Retrieve token mapping
	ownedAccounts, err := tokenCache.Get(context.Background(), owner)
	assert.NoError(t, err)
	assert.EqualValues(t, []ed25519.PublicKey{accounts[2]}, ownedAccounts)
}

func TestLoader_Update(t *testing.T) {
	accounts := testutil.GenerateSolanaKeys(t, 4)
	owner := testutil.GenerateSolanaKeys(t, 1)[0]

	sc := solana.NewMockClient()
	tc := token.NewClient(sc, accounts[0])

	accountInfoCache, err := infocache.NewCache(time.Minute, time.Minute, 100)
	require.NoError(t, err)
	tokenCache, err := tokencache.New(time.Minute, 30)
	require.NoError(t, err)

	loader := info.NewLoader(tc, accountInfoCache, tokenCache)
	for i, accountID := range accounts {
		err := loader.Update(context.Background(), owner, &account.AccountInfo{
			AccountId: &common.SolanaAccountId{
				Value: accountID,
			},
			Balance: int64(i),
		})
		require.NoError(t, err)
	}

	// Since overwrite overwrites entries
	tokenAccounts, err := tokenCache.Get(context.Background(), owner)
	assert.NoError(t, err)
	assert.ElementsMatch(t, accounts, tokenAccounts)

	for i, accountID := range accounts {
		info, err := loader.Load(context.Background(), accountID, solana.CommitmentRoot)
		assert.NoError(t, err)
		expected := &account.AccountInfo{
			AccountId: &common.SolanaAccountId{
				Value: accountID,
			},
			Balance: int64(i),
		}
		assert.True(t, proto.Equal(expected, info))
	}

	sc.AssertNotCalled(t, "GetAccountInfo")

}
