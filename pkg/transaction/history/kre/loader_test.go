package kre

import (
	"context"
	"crypto/ed25519"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/memo"
	"github.com/kinecosystem/agora-common/solana/system"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/kinecosystem/agora/pkg/account/solana/accountinfo"
	memoryaccount "github.com/kinecosystem/agora/pkg/account/solana/accountinfo/memory"
	"github.com/kinecosystem/agora/pkg/testutil"
	"github.com/kinecosystem/agora/pkg/transaction/history"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion/memory"
	memoryhistory "github.com/kinecosystem/agora/pkg/transaction/history/memory"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type testEnv struct {
	rw        history.ReaderWriter
	committer ingestion.Committer
	lock      *testLock

	mint ed25519.PublicKey
	sc   *solana.MockClient

	creationsSubmitter *submitter
	paymentsSubmitter  *submitter

	infoStore accountinfo.StateStore

	loader *Loader
}

func setup(t *testing.T) (env testEnv) {
	env.rw = memoryhistory.New()
	env.committer = memory.New()
	env.lock = &testLock{}
	env.mint = testutil.GenerateSolanaKeys(t, 1)[0]
	env.sc = solana.NewMockClient()
	env.creationsSubmitter = newTestSubmitter()
	env.paymentsSubmitter = newTestSubmitter()
	env.infoStore = memoryaccount.NewStore()

	env.loader = NewLoader(
		env.rw,
		env.committer,
		env.lock,
		env.sc,
		token.NewClient(env.sc, env.mint),
		env.creationsSubmitter,
		env.paymentsSubmitter,
		env.infoStore,
	)

	return env
}

func TestProcess_Fresh(t *testing.T) {
	env := setup(t)
	slot := uint64(2)
	pointer := model.OrderingKeyFromBlock(slot, false)
	require.NoError(t, env.committer.Commit(context.Background(), GetKREIngestorName(), nil, pointer))

	accounts := testutil.GenerateSolanaKeys(t, 4)

	// creation of [2] with owner as [3]
	// payment from [0] to [2]
	// payment from [2] to [0]
	instructions := []solana.Instruction{
		generateInstruction(t, env.mint, []ed25519.PublicKey{accounts[2], accounts[3]}, instructionTypeCreation, 0),
		generateInstruction(t, env.mint, []ed25519.PublicKey{accounts[0], accounts[2], accounts[1]}, instructionTypePayment, 10),
		generateInstruction(t, env.mint, []ed25519.PublicKey{accounts[2], accounts[0], accounts[3]}, instructionTypePayment, 3),
	}

	entry := generateSolanaEntry(t, 3, true, instructions)
	require.NoError(t, env.rw.Write(context.Background(), entry))

	// fetch slot once initially
	env.sc.On("GetSlot", mock.Anything).Return(uint64(3), nil).Once()
	// fetch slot during account update for [2]
	env.sc.On("GetSlot", mock.Anything).Return(uint64(4), nil).Once()
	// results in an error being thrown, cutting the tight loop short
	env.sc.On("GetSlot", mock.Anything).Return(uint64(0), errors.New("")).Once()
	env.sc.On("GetBlockTime", mock.Anything).Return(time.Now(), nil)

	// We expect get account info to get called for the destinations (amount ignored)
	env.sc.On("GetAccountInfo", accounts[0], solana.CommitmentSingle).Return(generateAccountInfo(10, env.mint, accounts[1], token.ProgramKey), nil).Once()
	env.sc.On("GetAccountInfo", accounts[2], solana.CommitmentSingle).Return(generateAccountInfo(0, env.mint, accounts[3], token.ProgramKey), nil).Once()

	// Called because no creates + not in store
	env.sc.On("GetAccountInfo", accounts[0], solana.CommitmentMax).Return(generateAccountInfo(20, env.mint, accounts[1], token.ProgramKey), nil).Once()

	assert.Error(t, env.loader.process(context.Background())) // expect an error from GetSlot returning an error

	assert.Equal(t, 1, len(env.creationsSubmitter.submitted))

	creations := env.creationsSubmitter.submitted[0].([]*creation)
	assert.Equal(t, 1, len(creations))

	assertCreation(t, creations[0], 0, accounts[2], accounts[3])

	assert.Equal(t, 1, len(env.paymentsSubmitter.submitted))

	payments := env.paymentsSubmitter.submitted[0].([]*payment)
	assert.Equal(t, 2, len(payments))

	assertPayment(t, payments[0], 1, 10, accounts[0], accounts[1], accounts[2], accounts[3])
	assertPayment(t, payments[1], 2, 3, accounts[2], accounts[3], accounts[0], accounts[1])

	// expected state:
	// [0]: owner=[1], slot=4, balance = 20 (slot + bal get fetched from solana)
	// [1]: get 0 as token account
	// [2]: owner[3], slot=3, balance= 0 + 10 - 3 = 7
	// [3]: get 2 as token account

	state0, err := env.infoStore.Get(context.Background(), accounts[0])
	require.NoError(t, err)
	assertState(t, state0, accounts[0], accounts[1], 20, 4)

	accounts1, err := env.infoStore.GetAccountsByOwner(context.Background(), accounts[1])
	require.NoError(t, err)
	assert.EqualValues(t, []ed25519.PublicKey{accounts[0]}, accounts1)

	state2, err := env.infoStore.Get(context.Background(), accounts[2])
	require.NoError(t, err)
	assertState(t, state2, accounts[2], accounts[3], 7, 3)

	accounts3, err := env.infoStore.GetAccountsByOwner(context.Background(), accounts[3])
	require.NoError(t, err)
	assert.EqualValues(t, []ed25519.PublicKey{accounts[2]}, accounts3)
}

func TestProcess_Existing(t *testing.T) {
	env := setup(t)
	slot := uint64(2)
	pointer := model.OrderingKeyFromBlock(slot, false)
	require.NoError(t, env.committer.Commit(context.Background(), GetKREIngestorName(), nil, pointer))

	accounts := testutil.GenerateSolanaKeys(t, 4)
	testutil.SortKeys(accounts) // for easier assertion later

	require.NoError(t, env.infoStore.Put(context.Background(), &accountinfo.State{
		Account: accounts[0],
		Owner:   accounts[1],
		Balance: 20,
		Slot:    3,
	}))
	require.NoError(t, env.infoStore.Put(context.Background(), &accountinfo.State{
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

	// fetch slot once initially
	env.sc.On("GetSlot", mock.Anything).Return(uint64(4), nil).Once()
	// results in an error being thrown, cutting the tight loop short
	env.sc.On("GetSlot", mock.Anything).Return(uint64(0), errors.New("")).Once()
	env.sc.On("GetBlockTime", mock.Anything).Return(time.Now(), nil)

	// We expect get account info to get called for ownership change for [0] + the destination [2]
	env.sc.On("GetAccountInfo", accounts[0], solana.CommitmentSingle).Return(generateAccountInfo(0, env.mint, accounts[3], token.ProgramKey), nil).Once()
	env.sc.On("GetAccountInfo", accounts[2], solana.CommitmentSingle).Return(generateAccountInfo(0, env.mint, accounts[3], token.ProgramKey), nil).Once()

	assert.Error(t, env.loader.process(context.Background())) // expect an error from GetSlot returning an error

	assert.Equal(t, 0, len(env.creationsSubmitter.submitted))
	assert.Equal(t, 1, len(env.paymentsSubmitter.submitted))
	payments := env.paymentsSubmitter.submitted[0].([]*payment)
	assert.Equal(t, 1, len(payments))
	assertPayment(t, payments[0], 3, 1, accounts[0], accounts[3], accounts[2], accounts[3])

	// expected state:
	// [0]: owner=[3], slot=4, balance = 20 - 1 = 19
	// [1]: get no token accounts
	// [2]: owner[3], slot=5, balance= 10 (unchanged since initial slot > current)
	// [3]: get [0, 2] as token accounts

	state0, err := env.infoStore.Get(context.Background(), accounts[0])
	require.NoError(t, err)
	assertState(t, state0, accounts[0], accounts[3], 19, 4)

	accounts1, err := env.infoStore.GetAccountsByOwner(context.Background(), accounts[1])
	require.NoError(t, err)
	assert.Empty(t, accounts1)

	state2, err := env.infoStore.Get(context.Background(), accounts[2])
	require.NoError(t, err)
	assertState(t, state2, accounts[2], accounts[3], 10, 5)

	accounts3, err := env.infoStore.GetAccountsByOwner(context.Background(), accounts[3])
	require.NoError(t, err)
	assert.Equal(t, 2, len(accounts3))
	testutil.SortKeys(accounts3)
	assert.EqualValues(t, []ed25519.PublicKey{accounts[0], accounts[2]}, accounts3)

	env.sc.AssertExpectations(t)
}

func TestMigrationTransaction(t *testing.T) {
	// generate
	env := setup(t)
	slot := uint64(2)
	pointer := model.OrderingKeyFromBlock(slot, false)
	require.NoError(t, env.committer.Commit(context.Background(), GetKREIngestorName(), nil, pointer))

	subsidizerKey := testutil.GenerateSolanaKeypair(t)
	subsidizer := subsidizerKey.Public().(ed25519.PublicKey)
	migrationAddress := testutil.GenerateSolanaKeys(t, 1)[0]
	tokenMint := testutil.GenerateSolanaKeys(t, 1)[0]
	owner := testutil.GenerateSolanaKeys(t, 1)[0]

	env.sc.On("GetAccountInfo", migrationAddress, solana.CommitmentSingle).Return(generateAccountInfo(0, env.mint, owner, token.ProgramKey), nil)

	txn := solana.NewTransaction(
		subsidizer,
		system.CreateAccount(
			subsidizer,
			migrationAddress,
			migrationAddress,
			10,
			10,
		),
		token.InitializeAccount(
			migrationAddress,
			env.mint,
			migrationAddress,
		),
		token.SetAuthority(
			migrationAddress,
			migrationAddress,
			subsidizer,
			token.AuthorityTypeCloseAccount,
		),
		token.SetAuthority(
			migrationAddress,
			migrationAddress,
			owner,
			token.AuthorityTypeAccountHolder,
		),
		token.Transfer(
			tokenMint,
			migrationAddress,
			tokenMint,
			10,
		),
	)
	assert.NoError(t, txn.Sign(subsidizerKey))

	entry := &model.Entry{
		Version: model.KinVersion_KIN4,
		Kind: &model.Entry_Solana{
			Solana: &model.SolanaEntry{
				Slot:        slot,
				Confirmed:   true,
				Transaction: txn.Marshal(),
			},
		},
	}

	require.NoError(t, env.rw.Write(context.Background(), entry))

	// fetch slot once initially
	env.sc.On("GetSlot", mock.Anything).Return(uint64(4), nil).Once()
	// results in an error being thrown, cutting the tight loop short
	env.sc.On("GetSlot", mock.Anything).Return(uint64(0), errors.New("")).Once()
	env.sc.On("GetBlockTime", mock.Anything).Return(time.Now(), nil)

	assert.Error(t, env.loader.process(context.Background())) // expect an error from GetSlot returning an error

	assert.Equal(t, 1, len(env.creationsSubmitter.submitted))
	creations := env.creationsSubmitter.submitted[0].([]*creation)
	assert.Equal(t, 1, len(creations))
	assert.Equal(t, migrationAddress, creations[0].account)
	assert.Equal(t, owner, creations[0].accountOwner)
	assert.Equal(t, subsidizer, creations[0].subsidizer)

	assert.Equal(t, 1, len(env.paymentsSubmitter.submitted))
	payments := env.paymentsSubmitter.submitted[0].([]*payment)
	assert.Equal(t, 1, len(payments))
	assertPayment(t, payments[0], 4, 10, tokenMint, tokenMint, migrationAddress, owner)

	env.sc.AssertExpectations(t)
}

type instructionType int

const (
	instructionTypeUnknown = iota
	instructionTypeCreation
	instructionTypePayment
	instructionTypeOwnership
	instructionTypeOtherAuthority
)

func generateInstruction(t *testing.T, mint ed25519.PublicKey, accounts []ed25519.PublicKey, instructionType instructionType, amount uint64) solana.Instruction {
	switch instructionType {
	case instructionTypeCreation:
		assert.Equal(t, 2, len(accounts))
		return token.InitializeAccount(accounts[0], mint, accounts[1])
	case instructionTypePayment:
		assert.Equal(t, 3, len(accounts))
		return token.Transfer(accounts[0], accounts[1], accounts[2], amount)
	case instructionTypeOwnership:
		assert.Equal(t, 3, len(accounts))
		return token.SetAuthority(accounts[0], accounts[1], accounts[2], token.AuthorityTypeAccountHolder)
	case instructionTypeOtherAuthority:
		assert.Equal(t, 3, len(accounts))
		return token.SetAuthority(accounts[0], accounts[1], accounts[2], token.AuthorityTypeCloseAccount)
	default:
		return memo.Instruction("data")
	}
}

func generateSolanaEntry(t *testing.T, slot uint64, confirmed bool, instructions []solana.Instruction) *model.Entry {
	signer := testutil.GenerateSolanaKeypair(t)
	txn := solana.NewTransaction(
		signer.Public().(ed25519.PublicKey),
		instructions...,
	)
	assert.NoError(t, txn.Sign(signer))

	return &model.Entry{
		Version: model.KinVersion_KIN4,
		Kind: &model.Entry_Solana{
			Solana: &model.SolanaEntry{
				Slot:        slot,
				Confirmed:   confirmed,
				Transaction: txn.Marshal(),
			},
		},
	}
}

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

func assertCreation(t *testing.T, creation *creation, offset int, account, owner ed25519.PublicKey) {
	assert.EqualValues(t, account, creation.account)
	assert.EqualValues(t, owner, creation.accountOwner)
	assert.Equal(t, offset, creation.offset)
}

func assertPayment(t *testing.T, payment *payment, offset int, quarks uint64, source, sourceOwner, dest, destOwner ed25519.PublicKey) {
	assert.Equal(t, offset, payment.offset)
	assert.EqualValues(t, source, payment.source)
	assert.EqualValues(t, sourceOwner, payment.sourceOwner)
	assert.EqualValues(t, dest, payment.dest)
	assert.EqualValues(t, destOwner, payment.destOwner)
	assert.EqualValues(t, quarks, payment.quarks)
}

func assertState(t *testing.T, state *accountinfo.State, account ed25519.PublicKey, owner ed25519.PublicKey, balance int64, slot uint64) {
	assert.EqualValues(t, account, state.Account)
	assert.EqualValues(t, owner, state.Owner)
	assert.EqualValues(t, balance, state.Balance)
	assert.EqualValues(t, slot, state.Slot)
}

type testLock struct {
	mu sync.Mutex
}

func (l *testLock) Lock(ctx context.Context) error {
	l.mu.Lock()
	return nil
}

func (l *testLock) Unlock() error {
	l.mu.Unlock()
	return nil
}

type submitter struct {
	sync.Mutex
	submitted []interface{}
}

func newTestSubmitter() *submitter {
	return &submitter{
		submitted: make([]interface{}, 0),
	}
}

func (s *submitter) Submit(ctx context.Context, src interface{}) (err error) {
	s.Lock()
	defer s.Unlock()

	s.submitted = append(s.submitted, src)
	return nil
}
