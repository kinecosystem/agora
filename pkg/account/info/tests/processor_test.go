package tests

import (
	"context"
	"crypto/ed25519"
	"testing"

	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/account/info"
	memoryaccount "github.com/kinecosystem/agora/pkg/account/info/memory"
	"github.com/kinecosystem/agora/pkg/testutil"
	"github.com/kinecosystem/agora/pkg/transaction/history"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

type testEnv struct {
	store     info.StateStore
	processor *info.StateProcessor
	mint      ed25519.PublicKey
	sc        *solana.MockClient
	tc        *token.Client
}

func setup(t *testing.T) (env testEnv) {
	env.mint = testutil.GenerateSolanaKeys(t, 1)[0]
	env.sc = solana.NewMockClient()
	env.tc = token.NewClient(env.sc, env.mint)
	env.store = memoryaccount.NewStore()

	env.processor = info.NewStateProcessor(
		env.store,
		env.sc,
		env.tc,
	)

	return env
}

func TestProcess_Fresh(t *testing.T) {
	env := setup(t)

	accounts := testutil.GenerateSolanaKeys(t, 7)
	owners := testutil.GenerateSolanaKeys(t, 7)
	for i := 2; i < len(accounts); i++ {
		env.sc.On("GetAccountInfo", accounts[i], solana.CommitmentMax).Return(generateAccountInfo(1000, env.mint, owners[i], token.ProgramKey), nil).Once()
	}

	env.sc.On("GetSlot", mock.Anything).Return(uint64(1), nil)

	sc := history.StateChange{
		LastKey: model.OrderingKeyFromBlock(2),
		Creations: []*history.Creation{
			{
				Account:      accounts[0],
				AccountOwner: owners[0],
			},
			{
				Account:      accounts[1],
				AccountOwner: owners[1],
			},
		},
		Payments: []*history.Payment{
			{
				Source:      accounts[2],
				SourceOwner: owners[2],
				Dest:        accounts[3],
				DestOwner:   owners[3],
				Quarks:      1000,
			},
			{
				Source:      accounts[4],
				SourceOwner: owners[4],
				Dest:        accounts[5],
				DestOwner:   owners[5],
				Quarks:      500,
			},
		},
		OwnershipChanges: []*history.OwnershipChange{
			{
				Address: accounts[6],
				Owner:   accounts[6],
			},
		},
	}

	assert.NoError(t, env.processor.UpdateState(context.Background(), sc))

	// Note: we expect the states (for the non-created) to be what
	//       we return in account info, because we cannot do any diff
	//       work without previous data.

	// todo: we should think about what it means to process fresh data.
	//
	// currently, we just take whatever value we load from GetAccount,
	// even if the slot is older than the processed one. If we assume
	// that the RPC nodes are consistent then we can make assumptions.
	// However, not all RPC nodes will be equally caught up, which could
	// cause some issues.
	for i := 0; i < len(accounts); i++ {
		expectedSlot := uint64(2)
		expectedBalance := int64(0)
		if i >= 2 {
			expectedSlot = 1
			expectedBalance = 1000
		}

		state, err := env.store.Get(context.Background(), accounts[i])
		require.NoError(t, err, "i: %v", i)
		assertState(t, state, accounts[i], owners[i], expectedBalance, expectedSlot)

		accounts, err := env.store.GetAccountsByOwner(context.Background(), owners[i])
		require.NoError(t, err)
		assert.EqualValues(t, []ed25519.PublicKey{accounts[0]}, accounts)
	}
}

/*
func TestProcess_Existing(t *testing.T) {
	env := setup(t)
	slot := uint64(2)
	pointer := model.OrderingKeyFromBlock(slot)
	require.NoError(t, env.committer.Commit(context.Background(), GetKREIngestorName(), nil, pointer))
	require.NoError(t, env.committer.Commit(context.Background(), ingestion.GetHistoryIngestorName(model.KinVersion_KIN4), nil, solanaingestion.PointerFromSlot(5)))

	accounts := testutil.GenerateSolanaKeys(t, 4)
	testutil.SortKeys(accounts) // for easier assertion later

	require.NoError(t, env.store.Put(context.Background(), &info.State{
		Account: accounts[0],
		Owner:   accounts[1],
		Balance: 20,
		Slot:    3,
	}))
	require.NoError(t, env.store.Put(context.Background(), &info.State{
		Account: accounts[2],
		Owner:   accounts[3],
		Balance: 10,
		Slot:    5,
	}))

	// ignored
	// ownership of [0] from [1] to [3]
	// another authority instruction that will be ignored
	// payment from [0] to [2]
	instructions := []solana.Instruction{
		generateInstruction(t, env.mint, nil, instructionTypeUnknown, 0),
		generateInstruction(t, env.mint, []ed25519.PublicKey{accounts[0], accounts[1], accounts[3]}, instructionTypeOwnership, 0),
		generateInstruction(t, env.mint, []ed25519.PublicKey{accounts[0], accounts[1], accounts[3]}, instructionTypeOtherAuthority, 0),
		generateInstruction(t, env.mint, []ed25519.PublicKey{accounts[0], accounts[2], accounts[3]}, instructionTypePayment, 1),
	}
	entry := generateSolanaEntry(t, 4, true, instructions)
	require.NoError(t, env.rw.Write(context.Background(), entry))

	env.sc.On("GetBlockTime", mock.Anything).Return(time.Now(), nil)

	// We expect get account info to get called for ownership change for [0] + the destination [2]
	env.sc.On("GetAccountInfo", accounts[0], solana.CommitmentSingle).Return(generateAccountInfo(0, env.mint, accounts[3], token.ProgramKey), nil).Once()
	env.sc.On("GetAccountInfo", accounts[2], solana.CommitmentSingle).Return(generateAccountInfo(0, env.mint, accounts[3], token.ProgramKey), nil).Once()

	assert.NoError(t, env.processor.process(context.Background()))

	assert.Equal(t, 0, len(env.creationsSubmitter.submitted))
	assert.Equal(t, 1, len(env.paymentsSubmitter.submitted))
	payments := env.paymentsSubmitter.submitted[0].([]*history.Payment)
	assert.Equal(t, 1, len(payments))
	assertPayment(t, payments[0], 3, 1, accounts[0], accounts[3], accounts[2], accounts[3])

	// expected state:
	// [0]: owner=[3], slot=4, balance = 20 - 1 = 19
	// [1]: get no token accounts
	// [2]: owner[3], slot=5, balance= 10 (unchanged since initial slot > current)
	// [3]: get [0, 2] as token accounts

	state0, err := env.store.Get(context.Background(), accounts[0])
	require.NoError(t, err)
	assertState(t, state0, accounts[0], accounts[3], 19, 4)

	accounts1, err := env.store.GetAccountsByOwner(context.Background(), accounts[1])
	require.NoError(t, err)
	assert.Empty(t, accounts1)

	state2, err := env.store.Get(context.Background(), accounts[2])
	require.NoError(t, err)
	assertState(t, state2, accounts[2], accounts[3], 10, 5)

	accounts3, err := env.store.GetAccountsByOwner(context.Background(), accounts[3])
	require.NoError(t, err)
	assert.Equal(t, 2, len(accounts3))
	testutil.SortKeys(accounts3)
	assert.EqualValues(t, []ed25519.PublicKey{accounts[0], accounts[2]}, accounts3)

	env.sc.AssertExpectations(t)
}
*/

func generateAccountInfo(amount uint64, mint, owner, accountOwner ed25519.PublicKey) solana.AccountInfo {
	ta := token.Account{
		Mint:   mint,
		Owner:  owner,
		Amount: amount,
	}
	return solana.AccountInfo{
		Data:  ta.Marshal(),
		Owner: accountOwner,
	}
}

func assertState(t *testing.T, state *info.State, account ed25519.PublicKey, owner ed25519.PublicKey, balance int64, slot uint64) {
	assert.EqualValues(t, account, state.Account)
	assert.EqualValues(t, owner, state.Owner)
	assert.EqualValues(t, balance, state.Balance)
	assert.EqualValues(t, slot, state.Slot)
}
