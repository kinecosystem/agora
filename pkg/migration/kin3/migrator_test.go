package kin3

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/kinecosystem/agora-common/headers"
	"github.com/kinecosystem/go/clients/horizon"
	hProtocol "github.com/kinecosystem/go/protocols/horizon"
	"github.com/kinecosystem/go/protocols/horizon/base"
	"github.com/kinecosystem/go/strkey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	xrate "golang.org/x/time/rate"

	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/system"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/kinecosystem/agora/pkg/migration"
	"github.com/kinecosystem/agora/pkg/migration/memory"
	"github.com/kinecosystem/agora/pkg/rate"
	"github.com/kinecosystem/agora/pkg/testutil"
	"github.com/kinecosystem/agora/pkg/version"
)

type kin3Env struct {
	sc    *solana.MockClient
	tc    *token.Client
	hc    *horizon.MockClient
	store migration.Store

	migrationSecret []byte
	token           ed25519.PublicKey

	subsidizerKey ed25519.PrivateKey

	mint    ed25519.PublicKey
	mintKey ed25519.PrivateKey

	migrator *kin3Migrator
}

func setupKin3Env(t *testing.T) (env kin3Env) {
	env.token = testutil.GenerateSolanaKeys(t, 1)[0]
	env.sc = solana.NewMockClient()
	env.tc = token.NewClient(env.sc, env.token)
	env.hc = &horizon.MockClient{}
	env.store = memory.New()
	env.migrationSecret = []byte("test")

	env.subsidizerKey = testutil.GenerateSolanaKeypair(t)
	env.mint = testutil.GenerateSolanaKeys(t, 1)[0]
	env.mintKey = testutil.GenerateSolanaKeypair(t)

	migrator := New(
		env.store,
		env.sc,
		env.hc,
		rate.NewLocalRateLimiter(xrate.Inf),
		env.token,
		env.subsidizerKey,
		env.mint,
		env.mintKey,
		env.migrationSecret,
	)
	env.migrator = migrator.(*kin3Migrator)
	return env
}

func TestMigrateAccount(t *testing.T) {
	env := setupKin3Env(t)

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
	env := setupKin3Env(t)

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

	hAccount := hProtocol.Account{
		Signers: []hProtocol.Signer{
			{
				Key:    strkey.MustEncode(strkey.VersionByteAccountID, testutil.GenerateSolanaKeys(t, 1)[0]),
				Weight: 1,
			},
		},
		Balances: []hProtocol.Balance{
			{
				Asset: base.Asset{
					Type: "native",
				},
				Balance: "100",
			},
		},
	}
	env.hc.On("LoadAccount", mock.Anything).Return(hAccount, nil)

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
		result, err := env.store.Get(ctx, account)
		assert.NoError(t, err)
		assert.Equal(t, tc.expectedStatus, result.Status)
		if result.Status > migration.StatusNone {
			assert.NotNil(t, result.Signature)
		}
	}
}

func TestLoadAccount(t *testing.T) {
	env := setupKin3Env(t)

	account := testutil.GenerateSolanaKeys(t, 1)[0]
	_, migrationKey, err := migration.DeriveMigrationAccount(account, env.migrationSecret)
	require.NoError(t, err)

	notFoundError := &horizon.Error{
		Problem: horizon.Problem{
			Status: http.StatusNotFound,
		},
	}

	//
	// Not Found
	//
	env.hc.On("LoadAccount", mock.Anything).Return(hProtocol.Account{}, notFoundError)
	_, err = env.migrator.loadAccount(context.Background(), account, migrationKey)
	assert.Equal(t, migration.ErrNotFound, err)

	//
	// Found - negative balance
	//
	hAccount := hProtocol.Account{
		Signers: []hProtocol.Signer{
			{
				Key:    strkey.MustEncode(strkey.VersionByteAccountID, account),
				Weight: 1,
			},
		},
		Balances: []hProtocol.Balance{
			{
				Asset: base.Asset{
					Type: "native",
				},
				Balance: "-1",
			},
		},
	}
	env.hc.ExpectedCalls = nil
	env.hc.On("LoadAccount", mock.Anything).Return(hAccount, nil)
	_, err = env.migrator.loadAccount(context.Background(), account, migrationKey)
	assert.Error(t, err)

	//
	// Found - burned
	//
	hAccount = hProtocol.Account{
		Signers: []hProtocol.Signer{
			{
				Key:    strkey.MustEncode(strkey.VersionByteAccountID, account),
				Weight: 0,
			},
		},
		Balances: []hProtocol.Balance{
			{
				Asset: base.Asset{
					Type: "native",
				},
				Balance: "100",
			},
		},
	}
	env.hc.ExpectedCalls = nil
	env.hc.On("LoadAccount", mock.Anything).Return(hAccount, nil)
	_, err = env.migrator.loadAccount(context.Background(), account, migrationKey)
	assert.Equal(t, migration.ErrBurned, err)

	//
	// Found, multi-sig
	//
	hAccount = hProtocol.Account{
		Signers: []hProtocol.Signer{
			{
				Key:    strkey.MustEncode(strkey.VersionByteAccountID, account),
				Weight: 1,
			},
			{
				Key:    strkey.MustEncode(strkey.VersionByteAccountID, testutil.GenerateSolanaKeys(t, 1)[0]),
				Weight: 1,
			},
		},
		Balances: []hProtocol.Balance{
			{
				Asset: base.Asset{
					Type: "native",
				},
				Balance: "100",
			},
		},
	}
	env.hc.ExpectedCalls = nil
	env.hc.On("LoadAccount", mock.Anything).Return(hAccount, nil)
	_, err = env.migrator.loadAccount(context.Background(), account, migrationKey)
	assert.Error(t, err)

	//
	// Found
	//
	hAccount = hProtocol.Account{
		Signers: []hProtocol.Signer{
			{
				Key:    strkey.MustEncode(strkey.VersionByteAccountID, account),
				Weight: 1,
			},
		},
		Balances: []hProtocol.Balance{
			{
				Asset: base.Asset{
					Type: "native",
				},
				Balance: "100",
			},
		},
	}
	env.hc.ExpectedCalls = nil
	env.hc.On("LoadAccount", mock.Anything).Return(hAccount, nil)
	info, err := env.migrator.loadAccount(context.Background(), account, migrationKey)
	assert.NoError(t, err)
	assert.Equal(t, uint64(100*1e5), info.balance)
	assert.Equal(t, account, info.account)
	assert.Equal(t, account, info.owner)
	assert.Equal(t, migrationKey.Public().(ed25519.PublicKey), info.migrationAccount)
	assert.Equal(t, migrationKey, info.migrationAccountKey)
}

func TestInitiateMigration_HorizonStates(t *testing.T) {
	env := setupKin3Env(t)

	ctx, err := headers.ContextWithHeaders(context.Background())
	require.NoError(t, err)
	require.NoError(t, headers.SetASCIIHeader(ctx, version.DesiredKinVersionHeader, "4"))

	account := testutil.GenerateSolanaKeys(t, 1)[0]
	require.NoError(t, err)

	migrationAccount, _, err := migration.DeriveMigrationAccount(account, env.migrationSecret)
	require.NoError(t, err)

	notFoundError := &horizon.Error{
		Problem: horizon.Problem{
			Status: http.StatusNotFound,
		},
	}

	//
	// Not Found
	//
	env.hc.On("LoadAccount", mock.Anything).Return(hProtocol.Account{}, notFoundError)
	assert.NoError(t, env.migrator.InitiateMigration(ctx, account, false, solana.CommitmentMax))
	state, err := env.store.Get(ctx, account)
	assert.NoError(t, err)
	assert.Equal(t, migration.ZeroState, state)
	count, err := env.store.GetCount(ctx, account)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	//
	// Found - Zero Balance
	//
	hAccount := hProtocol.Account{
		Signers: []hProtocol.Signer{
			{
				Key:    strkey.MustEncode(strkey.VersionByteAccountID, account),
				Weight: 1,
			},
		},
		Balances: []hProtocol.Balance{
			{
				Asset: base.Asset{
					Type: "native",
				},
				Balance: "0",
			},
		},
	}
	env.hc.ExpectedCalls = nil
	env.hc.On("LoadAccount", mock.Anything).Return(hAccount, nil)
	assert.NoError(t, env.migrator.InitiateMigration(ctx, account, false, solana.CommitmentMax))
	state, err = env.store.Get(ctx, account)
	assert.NoError(t, err)
	assert.Equal(t, migration.ZeroState, state)
	count, err = env.store.GetCount(ctx, account)
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	hAccount = hProtocol.Account{
		Signers: []hProtocol.Signer{
			{
				Key:    strkey.MustEncode(strkey.VersionByteAccountID, account),
				Weight: 1,
			},
		},
		Balances: []hProtocol.Balance{
			{
				Asset: base.Asset{
					Type: "native",
				},
				Balance: "10",
			},
		},
	}
	env.hc.ExpectedCalls = nil
	env.hc.On("LoadAccount", mock.Anything).Return(hAccount, nil)

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
	state, err = env.store.Get(ctx, account)
	assert.NoError(t, err)
	assert.Equal(t, migration.StatusComplete, state.Status)
	count, err = env.store.GetCount(ctx, account)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	transfer, err := token.DecompileTransferAccount(submitted.Message, 4)
	require.NoError(t, err)
	assert.Equal(t, env.mint, transfer.Source)
	assert.Equal(t, env.mintKey.Public().(ed25519.PublicKey), transfer.Owner)
	assert.Equal(t, migrationAccount, transfer.Destination)
	assert.Equal(t, uint64(10*1e5), transfer.Amount)
}

func TestInitiateMigration_AlreadyMigrated(t *testing.T) {
	env := setupKin3Env(t)

	ctx, err := headers.ContextWithHeaders(context.Background())
	require.NoError(t, err)
	require.NoError(t, headers.SetASCIIHeader(ctx, version.DesiredKinVersionHeader, "4"))

	account := testutil.GenerateSolanaKeys(t, 1)[0]
	require.NoError(t, err)

	assert.NoError(t, env.store.Update(ctx, account, migration.ZeroState, migration.State{Status: migration.StatusComplete}))

	// note: this will crash if any blockchain or horizon rpc is called as we have no setup mocks.
	assert.NoError(t, env.migrator.InitiateMigration(ctx, account, false, solana.CommitmentMax))

	count, err := env.store.GetCount(ctx, account)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestInitiateMigration_AlreadyMigrated_FromError(t *testing.T) {
	env := setupKin3Env(t)

	ctx, err := headers.ContextWithHeaders(context.Background())
	require.NoError(t, err)
	require.NoError(t, headers.SetASCIIHeader(ctx, version.DesiredKinVersionHeader, "4"))

	account := testutil.GenerateSolanaKeys(t, 1)[0]
	require.NoError(t, err)

	hAccount := hProtocol.Account{
		Signers: []hProtocol.Signer{
			{
				Key:    strkey.MustEncode(strkey.VersionByteAccountID, account),
				Weight: 1,
			},
		},
		Balances: []hProtocol.Balance{
			{
				Asset: base.Asset{
					Type: "native",
				},
				Balance: "10",
			},
		},
	}
	env.hc.ExpectedCalls = nil
	env.hc.On("LoadAccount", mock.Anything).Return(hAccount, nil)

	var signature solana.Signature
	_, err = io.CopyN(bytes.NewBuffer(signature[:]), rand.Reader, int64(len(signature[:])))
	require.NoError(t, err)

	env.sc.On("GetMinimumBalanceForRentExemption", mock.Anything).Return(10, nil)
	env.sc.On("GetRecentBlockhash").Return(solana.Blockhash{}, nil)

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
	state, err := env.store.Get(ctx, account)
	assert.NoError(t, err)
	assert.Equal(t, migration.StatusInProgress, state.Status)
	count, err := env.store.GetCount(ctx, account)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	// If we don't reset the store, the migrator will query the state first, rather than
	// check the transaction error. This is fine, but not what we're testing for here.
	env.store.(*memory.Store).Reset()

	assert.NoError(t, env.migrator.InitiateMigration(ctx, account, false, solana.CommitmentMax))
	state, err = env.store.Get(ctx, account)
	assert.NoError(t, err)
	assert.Equal(t, migration.StatusComplete, state.Status)
	count, err = env.store.GetCount(ctx, account)
	assert.NoError(t, err)
	assert.Equal(t, 2, count)

}
