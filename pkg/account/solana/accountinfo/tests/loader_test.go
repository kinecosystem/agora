package tests

import (
	"context"
	"testing"
	"time"

	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ed25519"

	"github.com/kinecosystem/agora/pkg/account/solana/accountinfo"
	accountinfocache "github.com/kinecosystem/agora/pkg/account/solana/accountinfo/memory"
	tokencache "github.com/kinecosystem/agora/pkg/account/solana/tokenaccount/memory"
	"github.com/kinecosystem/agora/pkg/testutil"
)

func TestLoader(t *testing.T) {
	accounts := testutil.GenerateSolanaKeys(t, 4)
	owner := testutil.GenerateSolanaKeys(t, 1)[0]

	sc := solana.NewMockClient()
	tc := token.NewClient(sc, accounts[0])

	accountInfoCache, err := accountinfocache.NewCache(time.Minute, time.Minute, 100)
	require.NoError(t, err)
	tokenCache, err := tokencache.New(time.Minute, 30)
	require.NoError(t, err)

	loader := accountinfo.NewLoader(tc, accountInfoCache, tokenCache)

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
	assert.Equal(t, err, accountinfo.ErrAccountInfoNotFound)
	_, err = loader.Load(context.Background(), accounts[3], solana.CommitmentMax)
	assert.Equal(t, err, accountinfo.ErrAccountInfoNotFound)

	info, err := loader.Load(context.Background(), accounts[2], solana.CommitmentMax)
	assert.NoError(t, err)
	assert.EqualValues(t, accounts[2], info.AccountId.Value)
	assert.EqualValues(t, 1, info.Balance)

	// writeback
	sc.ExpectedCalls = nil

	// Negative caching
	_, err = loader.Load(context.Background(), accounts[1], solana.CommitmentMax)
	assert.Equal(t, err, accountinfo.ErrAccountInfoNotFound)

	// Positive caching
	info, err = loader.Load(context.Background(), accounts[2], solana.CommitmentMax)
	assert.NoError(t, err)
	assert.EqualValues(t, accounts[2], info.AccountId.Value)
	assert.EqualValues(t, 1, info.Balance)

	// Retrieve token mapping
	ownedAccounts, err := tokenCache.Get(context.Background(), owner)
	assert.NoError(t, err)
	assert.EqualValues(t, []ed25519.PublicKey{accounts[2]}, ownedAccounts)
}
