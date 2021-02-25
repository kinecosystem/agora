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
	mints := testutil.GenerateSolanaKeys(t, 2)
	owners := testutil.GenerateSolanaKeys(t, 3)
	closeAuthority := testutil.GenerateSolanaKeys(t, 1)[0]

	sc := solana.NewMockClient()
	tc := token.NewClient(sc, mints[0])

	accountInfoCache, err := infocache.NewCache(time.Minute, time.Minute, 100)
	require.NoError(t, err)
	tokenCache, err := tokencache.New(time.Minute, 30)
	require.NoError(t, err)

	loader := info.NewLoader(tc, accountInfoCache, tokenCache)

	tokenInfo := token.Account{
		Mint:   mints[0],
		Owner:  owners[0],
		Amount: 1,
	}
	closeAuthorityTokenInfo := token.Account{
		Mint:           mints[0],
		Owner:          owners[1],
		CloseAuthority: closeAuthority,
		Amount:         2,
	}
	otherTokenInfo := token.Account{
		Mint:   mints[1],
		Owner:  owners[2],
		Amount: 1,
	}
	accountInfos := []solana.AccountInfo{
		{
			Data:  tokenInfo.Marshal(),
			Owner: token.ProgramKey,
		},
		{
			Data:  closeAuthorityTokenInfo.Marshal(),
			Owner: token.ProgramKey,
		},
		{
			Data:  otherTokenInfo.Marshal(),
			Owner: token.ProgramKey,
		},
	}

	sc.On("GetAccountInfo", accounts[0], mock.Anything).Return(accountInfos[0], nil)
	sc.On("GetAccountInfo", accounts[1], mock.Anything).Return(accountInfos[1], nil)
	sc.On("GetAccountInfo", accounts[2], mock.Anything).Return(accountInfos[2], nil)
	sc.On("GetAccountInfo", accounts[3], mock.Anything).Return(solana.AccountInfo{}, solana.ErrNoAccountInfo)

	// Not found is returned if there is no solana account, or the account is not for the mint.
	_, err = loader.Load(context.Background(), accounts[2], solana.CommitmentMax)
	assert.Equal(t, err, info.ErrAccountInfoNotFound)
	_, err = loader.Load(context.Background(), accounts[3], solana.CommitmentMax)
	assert.Equal(t, err, info.ErrAccountInfoNotFound)

	accountInfo, err := loader.Load(context.Background(), accounts[0], solana.CommitmentMax)
	assert.NoError(t, err)
	assert.EqualValues(t, accounts[0], accountInfo.AccountId.Value)
	assert.EqualValues(t, owners[0], accountInfo.Owner.Value)
	assert.EqualValues(t, owners[0], accountInfo.CloseAuthority.Value)
	assert.EqualValues(t, 1, accountInfo.Balance)

	accountInfo, err = loader.Load(context.Background(), accounts[1], solana.CommitmentMax)
	assert.NoError(t, err)
	assert.EqualValues(t, owners[1], accountInfo.Owner.Value)
	assert.EqualValues(t, closeAuthority, accountInfo.CloseAuthority.Value)
	assert.EqualValues(t, 2, accountInfo.Balance)

	// writeback
	sc.ExpectedCalls = nil

	// Negative caching
	_, err = loader.Load(context.Background(), accounts[2], solana.CommitmentMax)
	assert.Equal(t, err, info.ErrAccountInfoNotFound)

	// Positive caching
	accountInfo, err = loader.Load(context.Background(), accounts[1], solana.CommitmentMax)
	assert.NoError(t, err)
	assert.EqualValues(t, owners[1], accountInfo.Owner.Value)
	assert.EqualValues(t, closeAuthority, accountInfo.CloseAuthority.Value)
	assert.EqualValues(t, 2, accountInfo.Balance)

	// Retrieve token mapping
	ownedAccounts, err := tokenCache.Get(context.Background(), owners[0])
	assert.NoError(t, err)
	assert.EqualValues(t, []ed25519.PublicKey{accounts[0]}, ownedAccounts)
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
