package processor

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"sync"
	"testing"

	"github.com/golang/protobuf/ptypes"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/memo"
	"github.com/kinecosystem/agora-common/solana/system"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/testutil"
	"github.com/kinecosystem/agora/pkg/transaction/history"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion/memory"
	solanaingestion "github.com/kinecosystem/agora/pkg/transaction/history/ingestion/solana"
	memoryhistory "github.com/kinecosystem/agora/pkg/transaction/history/memory"
	model "github.com/kinecosystem/agora/pkg/transaction/history/model"
	historytestutil "github.com/kinecosystem/agora/pkg/transaction/history/model/testutil"
)

type testEnv struct {
	rw           history.ReaderWriter
	committer    ingestion.Committer
	lock         *testLock
	ingestorName string

	mint ed25519.PublicKey
	sc   *solana.MockClient
	tc   *token.Client

	processor *Processor

	called []history.StateChange
}

func setup(t *testing.T) (env *testEnv) {
	env = &testEnv{
		rw:           memoryhistory.New(),
		committer:    memory.New(),
		lock:         &testLock{},
		ingestorName: "test_ingestor",
		mint:         testutil.GenerateSolanaKeys(t, 1)[0],
		sc:           solana.NewMockClient(),
	}

	env.tc = token.NewClient(env.sc, env.mint)
	env.processor = NewProcessor(
		env.rw,
		env.committer,
		env.lock,
		env.ingestorName,
		env.tc,
		11,
		func(sc history.StateChange) error {
			env.called = append(env.called, sc)
			return nil
		},
	)

	return env
}

func TestInnerProcess(t *testing.T) {
	env := setup(t)

	sender := testutil.GenerateSolanaKeypair(t)
	accounts := testutil.GenerateSolanaKeys(t, 10)
	for _, a := range accounts {
		env.sc.On("GetAccountInfo", a, solana.CommitmentSingle).Return(generateAccountInfo(10, env.mint, a, token.ProgramKey), nil)
	}

	generated := make([]*model.Entry, 50)
	for i := 0; i < 50; i++ {
		var memo *string
		if i%2 != 0 {
			t := "hello"
			memo = &t
		}

		// To ensure that the ordering keys are not the same within a slot, we alternate
		// whether or not they have memo data inside.
		generated[i], _ = historytestutil.GenerateSolanaEntry(t, 1+uint64(i/2), i < 20*2, sender, accounts, nil, memo)
		require.NoError(t, env.rw.Write(context.Background(), generated[i]))
	}

	assert.NoError(t, env.committer.Commit(context.Background(), ingestion.GetHistoryIngestorName(model.KinVersion_KIN4), nil, solanaingestion.PointerFromSlot(20)))
	assert.NoError(t, env.processor.process(context.Background()))

	// Each ProcessRange() will act on 11 entries at a time.
	// There are 40 committed entries
	// Therefore, we expect ceil(40/11) callbacks.
	require.Equal(t, 4, len(env.called))
	for i, sc := range env.called {
		if i == len(env.called)-1 {
			assert.Equal(t, (40-11*3)*10, len(sc.Payments))
		} else {
			assert.Equal(t, 11*10, len(sc.Payments))
		}

		assert.Empty(t, sc.Creations)
		assert.Empty(t, sc.OwnershipChanges)
	}

	lastCommit, err := env.committer.Latest(context.Background(), env.ingestorName)
	assert.NoError(t, err)
	a, err := generated[38].GetOrderingKey()
	require.NoError(t, err)
	b, err := generated[39].GetOrderingKey()
	require.NoError(t, err)
	assert.True(t, bytes.Equal(lastCommit, a) || bytes.Equal(lastCommit, b))
}

func TestParsing(t *testing.T) {
	env := setup(t)

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

	// We expect get account info to get called for the destinations (amount ignored)
	env.sc.On("GetAccountInfo", accounts[0], solana.CommitmentSingle).Return(generateAccountInfo(10, env.mint, accounts[1], token.ProgramKey), nil).Once()
	env.sc.On("GetAccountInfo", accounts[2], solana.CommitmentSingle).Return(generateAccountInfo(0, env.mint, accounts[3], token.ProgramKey), nil).Once()

	sc, err := env.processor.ProcessRange(context.Background(), model.OrderingKeyFromBlock(0), model.OrderingKeyFromBlock(10), 1024)
	require.NoError(t, err)

	orderingKey, err := entry.GetOrderingKey()
	require.NoError(t, err)
	assert.Equal(t, orderingKey, sc.LastKey)

	assert.Equal(t, 1, len(sc.Creations))
	assertCreation(t, sc.Creations[0], 0, accounts[2], accounts[3])

	assert.Equal(t, 2, len(sc.Payments))
	assertPayment(t, sc.Payments[0], 1, 10, accounts[0], accounts[1], accounts[2], accounts[3])
	assertPayment(t, sc.Payments[1], 2, 3, accounts[2], accounts[3], accounts[0], accounts[1])
}

func TestMigrationTransaction(t *testing.T) {
	// generate
	env := setup(t)
	slot := uint64(2)

	subsidizerKey := testutil.GenerateSolanaKeypair(t)
	subsidizer := subsidizerKey.Public().(ed25519.PublicKey)
	migrationAddress := testutil.GenerateSolanaKeys(t, 1)[0]
	tokenMint := testutil.GenerateSolanaKeys(t, 1)[0]
	owner := testutil.GenerateSolanaKeys(t, 1)[0]

	// Lookup for payment (dest) as well as ownership transfer
	env.sc.On("GetAccountInfo", migrationAddress, solana.CommitmentSingle).Return(generateAccountInfo(0, env.mint, owner, token.ProgramKey), nil).Twice()

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
				BlockTime:   ptypes.TimestampNow(),
				Transaction: txn.Marshal(),
			},
		},
	}
	require.NoError(t, env.rw.Write(context.Background(), entry))

	sc, err := env.processor.ProcessRange(context.Background(), model.OrderingKeyFromBlock(0), model.OrderingKeyFromBlock(10), 0)
	assert.NoError(t, err)

	orderingKey, err := entry.GetOrderingKey()
	require.NoError(t, err)
	assert.Equal(t, orderingKey, sc.LastKey)

	assert.Equal(t, 1, len(sc.Creations))
	assert.Equal(t, migrationAddress, sc.Creations[0].Account)
	assert.Equal(t, owner, sc.Creations[0].AccountOwner)
	assert.Equal(t, subsidizer, sc.Creations[0].Subsidizer)

	assert.Equal(t, 1, len(sc.Payments))
	assertPayment(t, sc.Payments[0], 4, 10, tokenMint, tokenMint, migrationAddress, owner)

	env.sc.AssertExpectations(t)
}

type instructionType int

const (
	instructionTypeCreation = iota
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
				BlockTime:   ptypes.TimestampNow(),
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

func assertCreation(t *testing.T, creation *history.Creation, offset int, account, owner ed25519.PublicKey) {
	assert.EqualValues(t, account, creation.Account)
	assert.EqualValues(t, owner, creation.AccountOwner)
	assert.Equal(t, offset, creation.Offset)
}

func assertPayment(t *testing.T, payment *history.Payment, offset int, quarks uint64, source, sourceOwner, dest, destOwner ed25519.PublicKey) {
	assert.Equal(t, offset, payment.Offset)
	assert.EqualValues(t, source, payment.Source)
	assert.EqualValues(t, sourceOwner, payment.SourceOwner)
	assert.EqualValues(t, dest, payment.Dest)
	assert.EqualValues(t, destOwner, payment.DestOwner)
	assert.EqualValues(t, quarks, payment.Quarks)
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
