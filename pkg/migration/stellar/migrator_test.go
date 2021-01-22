package stellar

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/kinecosystem/agora-common/headers"
	"github.com/kinecosystem/agora-common/kin/version"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/system"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	xrate "golang.org/x/time/rate"

	"github.com/kinecosystem/agora/pkg/migration"
	"github.com/kinecosystem/agora/pkg/migration/memory"
	"github.com/kinecosystem/agora/pkg/rate"
	"github.com/kinecosystem/agora/pkg/testutil"
)

type testEnv struct {
	sc     *solana.MockClient
	tc     *token.Client
	loader *testLoader
	store  migration.Store

	migrationSecret []byte
	token           ed25519.PublicKey

	subsidizerKey ed25519.PrivateKey

	mint    ed25519.PublicKey
	mintKey ed25519.PrivateKey

	migrator *migrator
}

type accountState struct {
	owner   ed25519.PublicKey
	balance uint64
	err     error
}
type testLoader struct {
	sync.Mutex
	accountState map[string]*accountState
}

func (t *testLoader) LoadAccount(account ed25519.PublicKey) (ed25519.PublicKey, uint64, error) {
	t.Lock()
	defer t.Unlock()

	s, ok := t.accountState[string(account)]
	if !ok {
		return nil, 0, migration.ErrNotFound
	}

	return s.owner, s.balance, s.err
}

func setupEnv(t *testing.T) (env testEnv) {
	env.token = testutil.GenerateSolanaKeys(t, 1)[0]
	env.sc = solana.NewMockClient()
	env.tc = token.NewClient(env.sc, env.token)
	env.loader = &testLoader{accountState: make(map[string]*accountState)}
	env.store = memory.New()
	env.migrationSecret = []byte("test")

	env.subsidizerKey = testutil.GenerateSolanaKeypair(t)
	env.mint = testutil.GenerateSolanaKeys(t, 1)[0]
	env.mintKey = testutil.GenerateSolanaKeypair(t)

	m := New(
		env.store,
		version.KinVersion3,
		env.sc,
		env.loader,
		rate.NewLocalRateLimiter(xrate.Inf),
		env.token,
		env.subsidizerKey,
		env.mint,
		env.mintKey,
		env.migrationSecret,
	)
	env.migrator = m.(*migrator)
	return env
}

func TestDerivation(t *testing.T) {
	raw, err := base58.Decode("56TFPGGNL97wWLA9iiesmcbt9WRcMGmEFUbUqyXKPtaj")
	require.NoError(t, err)

	for _, s := range []string{
		"",
		"testmigrationkey1",
		"testmigrationkey2",
		"testmigrationkey3",
		"testmigrationkey4",
	} {
		pub, _, err := migration.DeriveMigrationAccount(raw, []byte(s))
		require.NoError(t, err)
		fmt.Printf("%s: %s\n", s, base58.Encode(pub))
	}
}

func TestMigrateAccount(t *testing.T) {
	env := setupEnv(t)

	info := accountInfo{
		account: testutil.GenerateSolanaKeys(t, 1)[0],
		owner:   testutil.GenerateSolanaKeys(t, 1)[0],
		balance: 10,
	}

	var err error
	info.migrationAccount, info.migrationAccountKey, err = migration.DeriveMigrationAccount(info.account, env.migrationSecret)
	require.NoError(t, err)

	var submitted solana.Transaction
	var signature solana.Signature
	_, err = io.CopyN(bytes.NewBuffer(signature[:]), rand.Reader, int64(len(signature[:])))
	require.NoError(t, err)

	env.sc.On("GetMinimumBalanceForRentExemption", mock.Anything).Return(10, nil)
	env.sc.On("GetRecentBlockhash").Return(solana.Blockhash{}, nil)
	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentRecent).Return(signature, &solana.SignatureStatus{}, nil).Run(func(args mock.Arguments) {
		submitted = args.Get(0).(solana.Transaction)
	})

	assert.NoError(t, env.migrator.migrateAccount(context.Background(), info, solana.CommitmentRecent))

	assert.Equal(t, 5, len(submitted.Message.Instructions))

	createAccount, err := system.DecompileCreateAccount(submitted.Message, 0)
	require.NoError(t, err)
	assert.Equal(t, info.migrationAccount, createAccount.Address)
	assert.Equal(t, token.ProgramKey, createAccount.Owner)
	assert.Equal(t, env.migrator.subsidizer, createAccount.Funder)

	initializeAccount, err := token.DecompileInitializeAccount(submitted.Message, 1)
	require.NoError(t, err)
	assert.Equal(t, info.migrationAccount, initializeAccount.Account)
	assert.Equal(t, env.token, initializeAccount.Mint)
	assert.Equal(t, info.migrationAccount, initializeAccount.Owner)

	setAuthority, err := token.DecompileSetAuthority(submitted.Message, 2)
	require.NoError(t, err)
	assert.Equal(t, info.migrationAccount, setAuthority.Account)
	assert.Equal(t, info.migrationAccount, setAuthority.CurrentAuthority)
	assert.Equal(t, env.migrator.subsidizer, setAuthority.NewAuthority)
	assert.Equal(t, token.AuthorityTypeCloseAccount, setAuthority.Type)

	setAuthority, err = token.DecompileSetAuthority(submitted.Message, 3)
	require.NoError(t, err)
	assert.Equal(t, info.migrationAccount, setAuthority.Account)
	assert.Equal(t, info.migrationAccount, setAuthority.CurrentAuthority)
	assert.Equal(t, info.owner, setAuthority.NewAuthority)
	assert.Equal(t, token.AuthorityTypeAccountHolder, setAuthority.Type)

	transfer, err := token.DecompileTransferAccount(submitted.Message, 4)
	require.NoError(t, err)
	assert.Equal(t, env.mint, transfer.Source)
	assert.Equal(t, env.mintKey.Public().(ed25519.PublicKey), transfer.Owner)
	assert.Equal(t, info.migrationAccount, transfer.Destination)
	assert.Equal(t, uint64(10), transfer.Amount)
}

func TestRecover(t *testing.T) {
	env := setupEnv(t)

	testCases := []struct {
		initialStatus  migration.Status
		expectedStatus migration.Status
		commitment     solana.Commitment
		accountExists  bool
	}{
		{
			initialStatus:  migration.StatusNone,
			expectedStatus: migration.StatusInProgress,
			commitment:     solana.CommitmentRecent,
		},
		{
			initialStatus:  migration.StatusNone,
			expectedStatus: migration.StatusComplete,
			commitment:     solana.CommitmentMax,
		},
		{
			initialStatus:  migration.StatusInProgress,
			expectedStatus: migration.StatusInProgress,
			commitment:     solana.CommitmentRecent,
		},
		{
			initialStatus:  migration.StatusInProgress,
			expectedStatus: migration.StatusComplete,
			commitment:     solana.CommitmentMax,
		},
		{
			initialStatus:  migration.StatusComplete,
			expectedStatus: migration.StatusComplete,
			commitment:     solana.CommitmentRecent,
		},
	}

	// Duplicate all of the above cases, but where the account already exists
	tcLen := len(testCases)
	for i := 0; i < tcLen; i++ {
		testCases = append(testCases, testCases[i])
		testCases[i+tcLen].accountExists = true
	}

	ctx, err := headers.ContextWithHeaders(context.Background())
	require.NoError(t, err)
	require.NoError(t, headers.SetASCIIHeader(ctx, version.DesiredKinVersionHeader, "4"))

	var signature solana.Signature
	_, err = io.CopyN(bytes.NewBuffer(signature[:]), rand.Reader, int64(len(signature[:])))
	require.NoError(t, err)

	confirmations := 5
	inProgressStatus := &solana.SignatureStatus{
		Confirmations: &confirmations,
	}
	completeStatus := &solana.SignatureStatus{}

	env.sc.On("GetMinimumBalanceForRentExemption", mock.Anything).Return(10, nil)
	env.sc.On("GetRecentBlockhash").Return(solana.Blockhash{}, nil)
	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentRecent).Return(signature, inProgressStatus, nil)
	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentMax).Return(signature, completeStatus, nil)

	// todo: stub out GetMinimumBalanceForRentExemption, GetRecentBlockHash, SubmitTransaction (dependent on commitment)
	for _, tc := range testCases {
		account := testutil.GenerateSolanaKeys(t, 1)[0]
		migrationAccount, _, err := migration.DeriveMigrationAccount(account, env.migrationSecret)
		require.NoError(t, err)
		env.loader.accountState[string(account)] = &accountState{
			owner:   account,
			balance: 100,
		}

		state := migration.State{
			Status: tc.initialStatus,
		}
		if tc.initialStatus != migration.StatusNone {
			state.LastModified = time.Now()
		}
		require.NoError(t, env.store.Update(ctx, account, migration.ZeroState, state))

		if tc.accountExists {
			sourceAccount := token.Account{
				Mint:   env.token,
				Owner:  account,
				Amount: 100,
				State:  token.AccountStateInitialized,
			}
			accountInfo := solana.AccountInfo{
				Owner: token.ProgramKey,
				Data:  sourceAccount.Marshal(),
			}
			env.sc.On("GetAccountInfo", migrationAccount, mock.Anything).Return(accountInfo, nil)
		} else {
			env.sc.On("GetAccountInfo", migrationAccount, mock.Anything).Return(solana.AccountInfo{}, solana.ErrNoAccountInfo)
		}

		assert.NoError(t, env.migrator.recover(ctx, account, migrationAccount, false, tc.commitment))

		// Verify final state
		result, exists, err := env.store.Get(ctx, account)
		assert.NoError(t, err)
		assert.Equal(t, tc.expectedStatus, result.Status)
		if result.Status > migration.StatusNone {
			assert.NotNil(t, result.Signature)
		}
		assert.True(t, exists)
	}
}

func TestLoadAccount(t *testing.T) {
	env := setupEnv(t)

	account := testutil.GenerateSolanaKeys(t, 1)[0]
	_, migrationKey, err := migration.DeriveMigrationAccount(account, env.migrationSecret)
	require.NoError(t, err)

	//
	// Found
	//
	env.loader.accountState[string(account)] = &accountState{
		owner:   account,
		balance: 100 * 1e5,
	}

	info, err := env.migrator.loadAccount(context.Background(), account, migrationKey)
	assert.NoError(t, err)
	assert.Equal(t, uint64(100*1e5), info.balance)
	assert.Equal(t, account, info.account)
	assert.Equal(t, account, info.owner)
	assert.Equal(t, migrationKey.Public().(ed25519.PublicKey), info.migrationAccount)
	assert.Equal(t, migrationKey, info.migrationAccountKey)
}

func TestInitiateMigration_HorizonStates(t *testing.T) {
	env := setupEnv(t)

	ctx, err := headers.ContextWithHeaders(context.Background())
	require.NoError(t, err)
	require.NoError(t, headers.SetASCIIHeader(ctx, version.DesiredKinVersionHeader, "4"))

	account := testutil.GenerateSolanaKeys(t, 1)[0]
	require.NoError(t, err)

	migrationAccount, _, err := migration.DeriveMigrationAccount(account, env.migrationSecret)
	require.NoError(t, err)

	//
	// Not Found
	//
	assert.Equal(t, migration.ErrNotFound, env.migrator.InitiateMigration(ctx, account, false, solana.CommitmentMax))
	state, exists, err := env.store.Get(ctx, account)
	assert.NoError(t, err)
	assert.Equal(t, migration.ZeroState, state)
	assert.False(t, exists)

	//
	// Found - Burned
	//
	env.loader.accountState[string(account)] = &accountState{
		owner:   account,
		balance: 10,
		err:     migration.ErrBurned,
	}
	assert.Equal(t, migration.ErrBurned, env.migrator.InitiateMigration(ctx, account, false, solana.CommitmentMax))
	state, exists, err = env.store.Get(ctx, account)
	assert.NoError(t, err)
	assert.Equal(t, migration.ZeroState, state)
	assert.False(t, exists)

	//
	// Found - Multisig
	//
	env.loader.accountState[string(account)] = &accountState{
		owner:   account,
		balance: 10,
		err:     migration.ErrMultisig,
	}
	assert.Equal(t, migration.ErrMultisig, env.migrator.InitiateMigration(ctx, account, false, solana.CommitmentMax))
	state, exists, err = env.store.Get(ctx, account)
	assert.NoError(t, err)
	assert.Equal(t, migration.ZeroState, state)
	assert.False(t, exists)

	//
	// Found - Zero Balance
	//
	env.loader.accountState[string(account)] = &accountState{
		owner:   account,
		balance: 0,
	}
	assert.NoError(t, env.migrator.InitiateMigration(ctx, account, false, solana.CommitmentMax))
	state, exists, err = env.store.Get(ctx, account)
	assert.NoError(t, err)
	assert.Equal(t, migration.ZeroState, state)
	assert.False(t, exists)

	env.loader.accountState[string(account)] = &accountState{
		owner:   account,
		balance: 10,
	}

	var submitted solana.Transaction
	var signature solana.Signature
	_, err = io.CopyN(bytes.NewBuffer(signature[:]), rand.Reader, int64(len(signature[:])))
	require.NoError(t, err)

	env.sc.On("GetMinimumBalanceForRentExemption", mock.Anything).Return(10, nil)
	env.sc.On("GetRecentBlockhash").Return(solana.Blockhash{}, nil)
	env.sc.On("SubmitTransaction", mock.Anything, mock.Anything).Return(signature, &solana.SignatureStatus{}, nil).Run(func(args mock.Arguments) {
		submitted = args.Get(0).(solana.Transaction)
	})

	assert.NoError(t, env.migrator.InitiateMigration(ctx, account, false, solana.CommitmentMax))
	state, exists, err = env.store.Get(ctx, account)
	assert.NoError(t, err)
	assert.Equal(t, migration.StatusComplete, state.Status)
	assert.True(t, exists)

	transfer, err := token.DecompileTransferAccount(submitted.Message, 4)
	require.NoError(t, err)
	assert.Equal(t, env.mint, transfer.Source)
	assert.Equal(t, env.mintKey.Public().(ed25519.PublicKey), transfer.Owner)
	assert.Equal(t, migrationAccount, transfer.Destination)
	assert.Equal(t, uint64(10), transfer.Amount)
}

func TestInitiateMigration_AlreadyMigrated(t *testing.T) {
	env := setupEnv(t)

	ctx, err := headers.ContextWithHeaders(context.Background())
	require.NoError(t, err)
	require.NoError(t, headers.SetASCIIHeader(ctx, version.DesiredKinVersionHeader, "4"))

	account := testutil.GenerateSolanaKeys(t, 1)[0]
	require.NoError(t, err)

	assert.NoError(t, env.store.Update(ctx, account, migration.ZeroState, migration.State{Status: migration.StatusComplete}))

	// note: this will crash if any blockchain or horizon rpc is called as we have no setup mocks.
	assert.NoError(t, env.migrator.InitiateMigration(ctx, account, false, solana.CommitmentMax))
}

func TestInitiateMigration_AlreadyMigrated_FromError(t *testing.T) {
	env := setupEnv(t)

	ctx, err := headers.ContextWithHeaders(context.Background())
	require.NoError(t, err)
	require.NoError(t, headers.SetASCIIHeader(ctx, version.DesiredKinVersionHeader, "4"))

	account := testutil.GenerateSolanaKeys(t, 1)[0]
	require.NoError(t, err)
	migrationAccount, _, err := migration.DeriveMigrationAccount(account, env.migrationSecret)
	require.NoError(t, err)

	env.loader.accountState[string(account)] = &accountState{
		owner:   account,
		balance: 10,
	}
	tokenAccount := token.Account{
		Mint:   env.token,
		Owner:  account,
		Amount: 10,
	}
	solanaAccount := solana.AccountInfo{
		Data:  tokenAccount.Marshal(),
		Owner: token.ProgramKey,
	}

	var signature solana.Signature
	_, err = io.CopyN(bytes.NewBuffer(signature[:]), rand.Reader, int64(len(signature[:])))
	require.NoError(t, err)

	env.sc.On("GetMinimumBalanceForRentExemption", mock.Anything).Return(10, nil)
	env.sc.On("GetRecentBlockhash").Return(solana.Blockhash{}, nil)
	env.sc.On("GetAccountInfo", migrationAccount, solana.CommitmentMax).Return(solanaAccount, nil)

	txnErr, err := solana.TransactionErrorFromInstructionError(&solana.InstructionError{
		Index: 0,
		Err:   solana.CustomError(0),
	})
	require.NoError(t, err)
	confirmations := 3
	status := &solana.SignatureStatus{
		ErrorResult:   txnErr,
		Confirmations: &confirmations,
	}
	env.sc.On("SubmitTransaction", mock.Anything, mock.Anything).Return(signature, status, nil)

	assert.NoError(t, env.migrator.InitiateMigration(ctx, account, false, solana.CommitmentRecent))
	state, exists, err := env.store.Get(ctx, account)
	assert.NoError(t, err)
	assert.Equal(t, migration.StatusInProgress, state.Status)
	assert.True(t, exists)

	// If we don't reset the store, the migrator will query the state first, rather than
	// check the transaction error. This is fine, but not what we're testing for here.
	env.store.(*memory.Store).Reset()

	assert.NoError(t, env.migrator.InitiateMigration(ctx, account, false, solana.CommitmentMax))
	state, exists, err = env.store.Get(ctx, account)
	assert.NoError(t, err)
	assert.Equal(t, migration.StatusComplete, state.Status)
	assert.True(t, exists)
}
