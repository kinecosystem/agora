package tests

import (
	"context"
	"testing"

	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/kinecosystem/agora/pkg/account"
	"github.com/kinecosystem/agora/pkg/account/memory"
	"github.com/kinecosystem/agora/pkg/testutil"
)

func TestMapper(t *testing.T) {
	mint := testutil.GenerateSolanaKeys(t, 1)[0]

	sc := solana.NewMockClient()
	tc := token.NewClient(sc, mint)

	mapper := memory.New()

	m := account.NewMapper(tc, mapper)

	accounts := testutil.GenerateSolanaKeys(t, 4)
	validTokenAccount := token.Account{
		Mint:   mint,
		Owner:  accounts[0],
		Amount: 10,
	}
	validAccount := solana.AccountInfo{
		Data:  validTokenAccount.Marshal(),
		Owner: token.ProgramKey,
	}
	invalidTokenAccount := token.Account{
		Mint:  accounts[3],
		Owner: token.ProgramKey,
	}
	invalidAccount := solana.AccountInfo{
		Data:  invalidTokenAccount.Marshal(),
		Owner: token.ProgramKey,
	}

	sc.On("GetAccountInfo", accounts[0], mock.Anything).Return(solana.AccountInfo{}, solana.ErrNoAccountInfo)
	sc.On("GetAccountInfo", accounts[1], mock.Anything).Return(invalidAccount, nil)
	sc.On("GetAccountInfo", accounts[2], mock.Anything).Return(validAccount, nil).Once()

	_, err := m.Get(context.Background(), accounts[0], solana.CommitmentRecent)
	assert.Equal(t, account.ErrNotFound, err)
	_, err = m.Get(context.Background(), accounts[1], solana.CommitmentRecent)
	assert.Equal(t, account.ErrNotFound, err)
	owner, err := m.Get(context.Background(), accounts[2], solana.CommitmentRecent)
	assert.NoError(t, err)
	assert.EqualValues(t, accounts[0], owner)

	// the cache should have cached the existence / mapping result
	sc.ExpectedCalls = nil
	owner, err = m.Get(context.Background(), accounts[2], solana.CommitmentRecent)
	assert.NoError(t, err)
	assert.EqualValues(t, accounts[0], owner)

	// manually adding should also work
	assert.NoError(t, m.Add(context.Background(), accounts[1], accounts[2]))
	owner, err = m.Get(context.Background(), accounts[1], solana.CommitmentRecent)
	assert.NoError(t, err)
	assert.EqualValues(t, accounts[2], owner)
}
