package server

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/agora-common/config"
	memconfig "github.com/kinecosystem/agora-common/config/memory"
	"github.com/kinecosystem/agora-common/headers"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/system"
	"github.com/kinecosystem/agora-common/solana/token"
	agoratestutil "github.com/kinecosystem/agora-common/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v4"
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v4"

	"github.com/kinecosystem/agora/pkg/account"
	"github.com/kinecosystem/agora/pkg/account/info"
	infodb "github.com/kinecosystem/agora/pkg/account/info/memory"
	"github.com/kinecosystem/agora/pkg/account/tokenaccount"
	"github.com/kinecosystem/agora/pkg/account/tokenaccount/memory"
	"github.com/kinecosystem/agora/pkg/migration"
	migrationstore "github.com/kinecosystem/agora/pkg/migration/memory"
	"github.com/kinecosystem/agora/pkg/testutil"
)

type mockAuthorizer struct {
	mock.Mock
}

func (m *mockAuthorizer) Authorize(ctx context.Context, tx solana.Transaction) (account.Authorization, error) {
	args := m.Called(ctx, tx)
	return args.Get(0).(account.Authorization), args.Error(1)
}

type testEnv struct {
	token      ed25519.PublicKey
	subsidizer ed25519.PrivateKey
	client     accountpb.AccountClient

	sc *solana.MockClient
	tc *token.Client

	notifier          *AccountNotifier
	tokenAccountCache tokenaccount.Cache
	infoCache         info.Cache
	migrationStore    migration.Store
	migrator          migration.Migrator
	auth              *mockAuthorizer

	server *server
}

func setup(t *testing.T, migrator migration.Migrator, conf ConfigProvider) (env testEnv, cleanup func()) {
	conn, serv, err := agoratestutil.NewServer(
		agoratestutil.WithUnaryServerInterceptor(headers.UnaryServerInterceptor()),
		agoratestutil.WithStreamServerInterceptor(headers.StreamServerInterceptor()),
	)
	require.NoError(t, err)

	env.subsidizer = testutil.GenerateSolanaKeypair(t)
	env.token = testutil.GenerateSolanaKeypair(t).Public().(ed25519.PublicKey)

	env.client = accountpb.NewAccountClient(conn)
	env.sc = solana.NewMockClient()
	env.tc = token.NewClient(env.sc, env.token)
	env.notifier = NewAccountNotifier()
	env.auth = &mockAuthorizer{}

	env.tokenAccountCache, err = memory.New(time.Hour, 5)
	require.NoError(t, err)

	env.infoCache, err = infodb.NewCache(time.Second, 2*time.Second, 5)
	require.NoError(t, err)

	env.migrationStore = migrationstore.New()
	if migrator != nil {
		env.migrator = migrator
	} else {
		env.migrator = migration.NewNoopMigrator()
	}

	env.server = New(
		conf,
		env.sc,
		env.notifier,
		env.tokenAccountCache,
		env.infoCache,
		info.NewLoader(env.tc, env.infoCache, env.tokenAccountCache),
		env.auth,
		migration.NewNoopLoader(),
		env.migrator,
		env.token,
		env.subsidizer,
		"",
		50,
	).(*server)

	serv.RegisterService(func(server *grpc.Server) {
		accountpb.RegisterAccountServer(server, env.server)
	})

	cleanup, err = serv.Serve()
	require.NoError(t, err)

	return env, cleanup
}

func TestMoveFront(t *testing.T) {
	for i := 0; i < 5; i++ {
		accounts := testutil.GenerateSolanaKeys(t, 5)
		sort.Sort(sortableAccounts(accounts))

		target := accounts[i]
		moveFront(accounts, target)
		assert.Equal(t, target, accounts[0])

		for i := 2; i < len(accounts); i++ {
			// This comparison also ensures that we have a unique set of accounts.
			assert.True(t, bytes.Compare(accounts[i-1], accounts[i]) < 0)
		}
	}
}

func TestCreateAccount(t *testing.T) {
	env, cleanup := setup(t, nil, WithOverrides(config.NoopConfig, memconfig.NewConfig(0.0)))
	defer cleanup()

	wallet := testutil.GenerateSolanaKeypair(t)
	owner := testutil.GenerateSolanaKeypair(t)

	createTxn := solana.NewTransaction(
		env.subsidizer.Public().(ed25519.PublicKey),
		system.CreateAccount(
			env.subsidizer.Public().(ed25519.PublicKey),
			wallet.Public().(ed25519.PublicKey),
			token.ProgramKey,
			env.server.minAccountLamports,
			token.AccountSize,
		),
		token.InitializeAccount(
			wallet.Public().(ed25519.PublicKey),
			env.token,
			owner.Public().(ed25519.PublicKey),
		),
		token.SetAuthority(
			wallet.Public().(ed25519.PublicKey),
			owner.Public().(ed25519.PublicKey),
			env.subsidizer.Public().(ed25519.PublicKey),
			token.AuthorityTypeCloseAccount,
		),
	)
	require.NoError(t, createTxn.Sign(env.subsidizer))

	var sig solana.Signature
	copy(sig[:], ed25519.Sign(env.subsidizer, createTxn.Marshal()))

	var submitted solana.Transaction
	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentMax).Return(sig, &solana.SignatureStatus{}, nil).Run(func(args mock.Arguments) {
		submitted = args.Get(0).(solana.Transaction)
	})

	authResult := account.Authorization{
		Address:        wallet.Public().(ed25519.PublicKey),
		Owner:          owner.Public().(ed25519.PublicKey),
		CloseAuthority: env.subsidizer.Public().(ed25519.PublicKey),
		Signature:      sig[:],
	}
	env.auth.On("Authorize", mock.Anything, createTxn).Return(authResult, nil)

	resp, err := env.client.CreateAccount(context.Background(), &accountpb.CreateAccountRequest{
		Transaction: &commonpb.Transaction{
			Value: createTxn.Marshal(),
		},
		Commitment: commonpb.Commitment_MAX,
	})
	require.NoError(t, err)
	assert.Equal(t, accountpb.CreateAccountResponse_OK, resp.Result)
	assert.EqualValues(t, owner.Public().(ed25519.PublicKey), resp.AccountInfo.Owner.Value)
	assert.EqualValues(t, env.subsidizer.Public().(ed25519.PublicKey), resp.AccountInfo.CloseAuthority.Value)

	cachedInfo, err := env.infoCache.Get(context.Background(), wallet.Public().(ed25519.PublicKey))
	assert.NoError(t, err)
	assert.EqualValues(t, wallet.Public().(ed25519.PublicKey), cachedInfo.AccountId.Value)
	assert.Zero(t, cachedInfo.Balance)

	expected := createTxn
	require.NoError(t, createTxn.Sign(env.subsidizer))
	assert.Equal(t, expected, submitted)

	txnErr, err := solana.TransactionErrorFromInstructionError(&solana.InstructionError{
		Index: 0,
		Err:   solana.CustomError(0),
	})
	require.NoError(t, err)
	status := &solana.SignatureStatus{
		ErrorResult: txnErr,
	}

	env.sc.ExpectedCalls = nil
	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentMax).Return(sig, status, nil)

	resp, err = env.client.CreateAccount(context.Background(), &accountpb.CreateAccountRequest{
		Transaction: &commonpb.Transaction{
			Value: createTxn.Marshal(),
		},
		Commitment: commonpb.Commitment_MAX,
	})
	require.NoError(t, err)
	assert.Equal(t, accountpb.CreateAccountResponse_OK, resp.Result)
}

func TestCreateAccount_AssociatedAccount(t *testing.T) {
	env, cleanup := setup(t, nil, WithOverrides(config.NoopConfig, memconfig.NewConfig(0.0)))
	defer cleanup()

	wallet := testutil.GenerateSolanaKeypair(t)
	owner := testutil.GenerateSolanaKeypair(t)

	createTxn := solana.NewTransaction(
		env.subsidizer.Public().(ed25519.PublicKey),
		system.CreateAccount(
			env.subsidizer.Public().(ed25519.PublicKey),
			wallet.Public().(ed25519.PublicKey),
			token.ProgramKey,
			env.server.minAccountLamports,
			token.AccountSize,
		),
		token.InitializeAccount(
			wallet.Public().(ed25519.PublicKey),
			env.token,
			owner.Public().(ed25519.PublicKey),
		),
		token.SetAuthority(
			wallet.Public().(ed25519.PublicKey),
			owner.Public().(ed25519.PublicKey),
			env.subsidizer.Public().(ed25519.PublicKey),
			token.AuthorityTypeCloseAccount,
		),
	)
	require.NoError(t, createTxn.Sign(env.subsidizer))

	var sig solana.Signature
	copy(sig[:], ed25519.Sign(env.subsidizer, createTxn.Marshal()))

	var submitted solana.Transaction
	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentMax).Return(sig, &solana.SignatureStatus{}, nil).Run(func(args mock.Arguments) {
		submitted = args.Get(0).(solana.Transaction)
	})

	authResult := account.Authorization{
		Address:        wallet.Public().(ed25519.PublicKey),
		Owner:          owner.Public().(ed25519.PublicKey),
		CloseAuthority: env.subsidizer.Public().(ed25519.PublicKey),
		Signature:      sig[:],
	}
	env.auth.On("Authorize", mock.Anything, createTxn).Return(authResult, nil)

	resp, err := env.client.CreateAccount(context.Background(), &accountpb.CreateAccountRequest{
		Transaction: &commonpb.Transaction{
			Value: createTxn.Marshal(),
		},
		Commitment: commonpb.Commitment_MAX,
	})
	require.NoError(t, err)
	assert.Equal(t, accountpb.CreateAccountResponse_OK, resp.Result)

	cachedInfo, err := env.infoCache.Get(context.Background(), wallet.Public().(ed25519.PublicKey))
	assert.NoError(t, err)
	assert.EqualValues(t, wallet.Public().(ed25519.PublicKey), cachedInfo.AccountId.Value)
	assert.Zero(t, cachedInfo.Balance)

	expected := createTxn
	require.NoError(t, createTxn.Sign(env.subsidizer))
	assert.Equal(t, expected, submitted)

	txnErr, err := solana.TransactionErrorFromInstructionError(&solana.InstructionError{
		Index: 0,
		Err:   solana.CustomError(0),
	})
	require.NoError(t, err)
	status := &solana.SignatureStatus{
		ErrorResult: txnErr,
	}

	env.sc.ExpectedCalls = nil
	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentMax).Return(sig, status, nil)

	resp, err = env.client.CreateAccount(context.Background(), &accountpb.CreateAccountRequest{
		Transaction: &commonpb.Transaction{
			Value: createTxn.Marshal(),
		},
		Commitment: commonpb.Commitment_MAX,
	})
	require.NoError(t, err)
	assert.Equal(t, accountpb.CreateAccountResponse_OK, resp.Result)
}

func TestCreateAccount_Exists(t *testing.T) {
	env, cleanup := setup(t, nil, WithOverrides(config.NoopConfig, memconfig.NewConfig(0.0)))
	defer cleanup()

	wallet := testutil.GenerateSolanaKeypair(t)
	owner := testutil.GenerateSolanaKeypair(t)

	createTxn := solana.NewTransaction(
		env.subsidizer.Public().(ed25519.PublicKey),
		system.CreateAccount(
			env.subsidizer.Public().(ed25519.PublicKey),
			wallet.Public().(ed25519.PublicKey),
			token.ProgramKey,
			env.server.minAccountLamports,
			token.AccountSize,
		),
		token.InitializeAccount(
			wallet.Public().(ed25519.PublicKey),
			env.token,
			owner.Public().(ed25519.PublicKey),
		),
		token.SetAuthority(
			wallet.Public().(ed25519.PublicKey),
			owner.Public().(ed25519.PublicKey),
			env.subsidizer.Public().(ed25519.PublicKey),
			token.AuthorityTypeCloseAccount,
		),
	)
	require.NoError(t, createTxn.Sign(env.subsidizer))

	err := env.server.loader.Update(context.Background(), owner.Public().(ed25519.PublicKey), &accountpb.AccountInfo{
		AccountId: &commonpb.SolanaAccountId{
			Value: wallet.Public().(ed25519.PublicKey),
		},
		Balance: 20,
	})
	require.NoError(t, err)

	var sig solana.Signature
	copy(sig[:], ed25519.Sign(env.subsidizer, createTxn.Marshal()))

	authResult := account.Authorization{
		Address:        wallet.Public().(ed25519.PublicKey),
		Owner:          owner.Public().(ed25519.PublicKey),
		CloseAuthority: env.subsidizer.Public().(ed25519.PublicKey),
		Signature:      sig[:],
	}
	env.auth.On("Authorize", mock.Anything, createTxn).Return(authResult, nil)

	txErr, err := solana.TransactionErrorFromInstructionError(&solana.InstructionError{
		Index: 0,
		Err:   solana.CustomError(0),
	})
	require.NoError(t, err)
	sigStatus := &solana.SignatureStatus{
		ErrorResult: txErr,
	}
	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentMax).Return(sig, sigStatus, nil)

	resp, err := env.client.CreateAccount(context.Background(), &accountpb.CreateAccountRequest{
		Transaction: &commonpb.Transaction{
			Value: createTxn.Marshal(),
		},
		Commitment: commonpb.Commitment_MAX,
	})
	require.NoError(t, err)
	assert.Equal(t, accountpb.CreateAccountResponse_OK, resp.Result)
	assert.EqualValues(t, wallet.Public().(ed25519.PublicKey), resp.AccountInfo.AccountId.Value)
	assert.EqualValues(t, 20, resp.AccountInfo.Balance)
}

func TestCreateAccount_NoSubsidizer(t *testing.T) {
	env, cleanup := setup(t, nil, WithOverrides(config.NoopConfig, memconfig.NewConfig(0.0)))
	defer cleanup()

	wallet := testutil.GenerateSolanaKeypair(t)
	owner := testutil.GenerateSolanaKeypair(t)
	env.server.subsidizer = nil

	createTxn := solana.NewTransaction(
		env.subsidizer.Public().(ed25519.PublicKey),
		system.CreateAccount(
			env.subsidizer.Public().(ed25519.PublicKey),
			wallet.Public().(ed25519.PublicKey),
			token.ProgramKey,
			env.server.minAccountLamports,
			token.AccountSize,
		),
		token.InitializeAccount(
			wallet.Public().(ed25519.PublicKey),
			env.token,
			owner.Public().(ed25519.PublicKey),
		),
	)
	require.NoError(t, createTxn.Sign(env.subsidizer))

	var sig solana.Signature
	copy(sig[:], ed25519.Sign(env.subsidizer, createTxn.Marshal()))

	authResult := account.Authorization{
		Result:  account.AuthorizationResultPayerRequired,
		Address: wallet.Public().(ed25519.PublicKey),
		Owner:   owner.Public().(ed25519.PublicKey),
	}
	env.auth.On("Authorize", mock.Anything, createTxn).Return(authResult, nil)

	resp, err := env.client.CreateAccount(context.Background(), &accountpb.CreateAccountRequest{
		Transaction: &commonpb.Transaction{
			Value: createTxn.Marshal(),
		},
		Commitment: commonpb.Commitment_MAX,
	})
	require.NoError(t, err)
	assert.Equal(t, accountpb.CreateAccountResponse_PAYER_REQUIRED, resp.Result)
}

func TestCreateAccount_InvalidBlockhash(t *testing.T) {
	env, cleanup := setup(t, nil, WithOverrides(config.NoopConfig, memconfig.NewConfig(0.0)))
	defer cleanup()

	wallet := testutil.GenerateSolanaKeypair(t)
	owner := testutil.GenerateSolanaKeypair(t)
	env.server.subsidizer = nil

	createTxn := solana.NewTransaction(
		env.subsidizer.Public().(ed25519.PublicKey),
		system.CreateAccount(
			env.subsidizer.Public().(ed25519.PublicKey),
			wallet.Public().(ed25519.PublicKey),
			token.ProgramKey,
			env.server.minAccountLamports,
			token.AccountSize,
		),
		token.InitializeAccount(
			wallet.Public().(ed25519.PublicKey),
			env.token,
			owner.Public().(ed25519.PublicKey),
		),
	)
	require.NoError(t, createTxn.Sign(env.subsidizer))

	var sig solana.Signature
	copy(sig[:], ed25519.Sign(env.subsidizer, createTxn.Marshal()))

	sigStatus := &solana.SignatureStatus{
		ErrorResult: solana.NewTransactionError(solana.TransactionErrorBlockhashNotFound),
	}
	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentMax).Return(sig, sigStatus, nil)
	authResult := account.Authorization{
		Address:   wallet.Public().(ed25519.PublicKey),
		Owner:     owner.Public().(ed25519.PublicKey),
		Signature: sig[:],
	}
	env.auth.On("Authorize", mock.Anything, createTxn).Return(authResult, nil)

	resp, err := env.client.CreateAccount(context.Background(), &accountpb.CreateAccountRequest{
		Transaction: &commonpb.Transaction{
			Value: createTxn.Marshal(),
		},
		Commitment: commonpb.Commitment_MAX,
	})
	require.NoError(t, err)
	assert.Equal(t, accountpb.CreateAccountResponse_BAD_NONCE, resp.Result)
}

func TestCreateAccount_Limited(t *testing.T) {
	env, cleanup := setup(t, nil, WithOverrides(config.NoopConfig, memconfig.NewConfig(0.0)))
	defer cleanup()

	wallet := testutil.GenerateSolanaKeypair(t)
	owner := testutil.GenerateSolanaKeypair(t)

	createTxn := solana.NewTransaction(
		env.subsidizer.Public().(ed25519.PublicKey),
		system.CreateAccount(
			env.subsidizer.Public().(ed25519.PublicKey),
			wallet.Public().(ed25519.PublicKey),
			token.ProgramKey,
			env.server.minAccountLamports,
			token.AccountSize,
		),
		token.InitializeAccount(
			wallet.Public().(ed25519.PublicKey),
			env.token,
			owner.Public().(ed25519.PublicKey),
		),
		token.SetAuthority(
			wallet.Public().(ed25519.PublicKey),
			wallet.Public().(ed25519.PublicKey),
			env.subsidizer.Public().(ed25519.PublicKey),
			token.AuthorityTypeCloseAccount,
		),
	)
	require.NoError(t, createTxn.Sign(env.subsidizer))

	var sig solana.Signature
	copy(sig[:], ed25519.Sign(env.subsidizer, createTxn.Marshal()))

	env.auth.On("Authorize", mock.Anything, createTxn).Return(account.Authorization{}, status.Error(codes.ResourceExhausted, ""))
	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentMax).Return(sig, &solana.SignatureStatus{}, nil)

	_, err := env.client.CreateAccount(context.Background(), &accountpb.CreateAccountRequest{
		Transaction: &commonpb.Transaction{
			Value: createTxn.Marshal(),
		},
		Commitment: commonpb.Commitment_MAX,
	})
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
}

func TestCreateAccount_Whitelisting(t *testing.T) {
	env, cleanup := setup(t, nil, WithOverrides(config.NoopConfig, memconfig.NewConfig(0.0)))
	defer cleanup()

	env.server.createWhitelistSecret = "somesecret"

	wallet := testutil.GenerateSolanaKeypair(t)
	owner := testutil.GenerateSolanaKeypair(t)

	createTxn := solana.NewTransaction(
		env.subsidizer.Public().(ed25519.PublicKey),
		system.CreateAccount(
			env.subsidizer.Public().(ed25519.PublicKey),
			wallet.Public().(ed25519.PublicKey),
			token.ProgramKey,
			env.server.minAccountLamports,
			token.AccountSize,
		),
		token.InitializeAccount(
			wallet.Public().(ed25519.PublicKey),
			env.token,
			owner.Public().(ed25519.PublicKey),
		),
		token.SetAuthority(
			wallet.Public().(ed25519.PublicKey),
			owner.Public().(ed25519.PublicKey),
			env.subsidizer.Public().(ed25519.PublicKey),
			token.AuthorityTypeCloseAccount,
		),
	)
	require.NoError(t, createTxn.Sign(env.subsidizer))

	var sig solana.Signature
	copy(sig[:], ed25519.Sign(env.subsidizer, createTxn.Marshal()))

	authResult := account.Authorization{
		Address:        wallet.Public().(ed25519.PublicKey),
		Owner:          owner.Public().(ed25519.PublicKey),
		CloseAuthority: env.subsidizer.Public().(ed25519.PublicKey),
		Signature:      sig[:],
	}
	env.auth.On("Authorize", mock.Anything, createTxn).Return(authResult, nil)
	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentMax).Return(sig, &solana.SignatureStatus{}, nil)

	resp, err := env.client.CreateAccount(context.Background(), &accountpb.CreateAccountRequest{
		Transaction: &commonpb.Transaction{
			Value: createTxn.Marshal(),
		},
		Commitment: commonpb.Commitment_MAX,
	})
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	assert.Nil(t, resp)

	for _, invalidUA := range []string{
		"",
		"haxor",
	} {
		md := map[string]string{
			"kin-user-agent": invalidUA,
		}

		ctx := metadata.NewOutgoingContext(context.Background(), metadata.New(md))
		resp, err := env.client.CreateAccount(ctx, &accountpb.CreateAccountRequest{
			Transaction: &commonpb.Transaction{
				Value: createTxn.Marshal(),
			},
			Commitment: commonpb.Commitment_MAX,
		})
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))
		assert.Nil(t, resp)
	}

	for _, validUA := range []string{
		env.server.createWhitelistSecret,
		"KinSDK/0.2.3_node/v10.23.0",
		"JVM/unspecified KinSDK/xyz CID/xyz",
		"iOS/10_1 iPad5,2 CFNetwork/808.3 Darwin/16.3.0 KinSDK/xyz CID/xyz",
	} {
		fmt.Println(validUA)
		md := map[string]string{
			"kin-user-agent": validUA,
		}

		ctx := metadata.NewOutgoingContext(context.Background(), metadata.New(md))
		resp, err := env.client.CreateAccount(ctx, &accountpb.CreateAccountRequest{
			Transaction: &commonpb.Transaction{
				Value: createTxn.Marshal(),
			},
			Commitment: commonpb.Commitment_MAX,
		})
		require.NoError(t, err)
		assert.Equal(t, accountpb.CreateAccountResponse_OK, resp.Result)
	}
}

func TestGetAccountInfo(t *testing.T) {
	env, cleanup := setup(t, nil, WithOverrides(config.NoopConfig, memconfig.NewConfig(0.0)))
	defer cleanup()

	accounts := testutil.GenerateSolanaKeys(t, 4)
	validTokenAccount := token.Account{
		Mint:   env.token,
		Owner:  token.ProgramKey,
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

	env.sc.On("GetAccountInfo", accounts[0], mock.Anything).Return(solana.AccountInfo{}, solana.ErrNoAccountInfo)
	env.sc.On("GetAccountInfo", accounts[1], mock.Anything).Return(invalidAccount, nil)
	env.sc.On("GetAccountInfo", accounts[2], mock.Anything).Return(validAccount, nil).Once()

	resp, err := env.client.GetAccountInfo(context.Background(), &accountpb.GetAccountInfoRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: accounts[0],
		},
		Commitment: commonpb.Commitment_MAX,
	})
	assert.NoError(t, err)
	assert.Equal(t, accountpb.GetAccountInfoResponse_NOT_FOUND, resp.Result)

	resp, err = env.client.GetAccountInfo(context.Background(), &accountpb.GetAccountInfoRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: accounts[1],
		},
		Commitment: commonpb.Commitment_MAX,
	})
	assert.NoError(t, err)
	assert.Equal(t, accountpb.GetAccountInfoResponse_NOT_FOUND, resp.Result)

	resp, err = env.client.GetAccountInfo(context.Background(), &accountpb.GetAccountInfoRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: accounts[2],
		},
		Commitment: commonpb.Commitment_MAX,
	})
	assert.NoError(t, err)
	assert.Equal(t, accountpb.GetAccountInfoResponse_OK, resp.Result)
	assert.EqualValues(t, accounts[2], resp.AccountInfo.AccountId.Value)
	assert.EqualValues(t, 10, resp.AccountInfo.Balance)

	// Verify cached
	resp, err = env.client.GetAccountInfo(context.Background(), &accountpb.GetAccountInfoRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: accounts[2],
		},
		Commitment: commonpb.Commitment_MAX,
	})
	assert.NoError(t, err)
	assert.Equal(t, accountpb.GetAccountInfoResponse_OK, resp.Result)
	assert.EqualValues(t, accounts[2], resp.AccountInfo.AccountId.Value)
	assert.EqualValues(t, 10, resp.AccountInfo.Balance)

	env.sc.AssertExpectations(t)
}

func TestResolveTokenAccounts(t *testing.T) {
	env, cleanup := setup(t, nil, WithOverrides(config.NoopConfig, memconfig.NewConfig(0.0)))
	defer cleanup()

	// note: we create a reverse() function to ensure that the impl
	//       does proper sorting on the results from the underlying client.
	accounts := testutil.GenerateSolanaKeys(t, 5)
	accountsWithAssoc := testutil.GenerateSolanaKeys(t, 5)
	accountsWithAssocAndIdentity := testutil.GenerateSolanaKeys(t, 5)

	assocAddr, err := token.GetAssociatedAccount(accountsWithAssoc[0], env.token)
	require.NoError(t, err)
	accountsWithAssoc[4] = assocAddr

	assocAddrWithIdentity, err := token.GetAssociatedAccount(accountsWithAssocAndIdentity[0], env.token)
	require.NoError(t, err)
	accountsWithAssocAndIdentity[4] = assocAddrWithIdentity

	reverse := func(keys []ed25519.PublicKey) []ed25519.PublicKey {
		r := make([]ed25519.PublicKey, len(keys))
		for i := range keys {
			r[len(keys)-1-i] = keys[i]
		}
		return r
	}

	// Accounts[0] -> set of accounts, non-identity, no assoc
	env.sc.On("GetTokenAccountsByOwner", accounts[0], env.token).Return(reverse(accounts[1:]), nil)
	// Accounts[1] -> set of accounts, _with_ identity
	env.sc.On("GetTokenAccountsByOwner", accounts[1], env.token).Return(reverse(accounts[1:]), nil)
	// Accounts[2] -> set of accounts, _with_ associated account
	env.sc.On("GetTokenAccountsByOwner", accountsWithAssoc[0], env.token).Return(reverse(accountsWithAssoc[1:]), nil)
	// Accounts[3] -> set of accounts, _with_ identity _and_ associated account
	env.sc.On("GetTokenAccountsByOwner", accountsWithAssocAndIdentity[0], env.token).Return(reverse(accountsWithAssocAndIdentity[0:]), nil)
	// Fallback
	env.sc.On("GetTokenAccountsByOwner", mock.Anything, env.token).Return([]ed25519.PublicKey{}, nil)

	resp, err := env.client.ResolveTokenAccounts(context.Background(), &accountpb.ResolveTokenAccountsRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: accounts[0],
		},
	})
	assert.NoError(t, err)
	for _, account := range resp.TokenAccounts {
		var found bool
		for _, existing := range accounts[1:] {
			if bytes.Equal(account.Value, existing) {
				found = true
				break
			}
		}
		assert.True(t, found)
	}
	for i := 1; i < len(resp.TokenAccounts); i++ {
		assert.True(t, bytes.Compare(resp.TokenAccounts[i-1].Value, resp.TokenAccounts[i].Value) < 0)
	}

	resp, err = env.client.ResolveTokenAccounts(context.Background(), &accountpb.ResolveTokenAccountsRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: accounts[1],
		},
	})
	assert.NoError(t, err)
	assert.EqualValues(t, accounts[1], resp.TokenAccounts[0].Value)
	for _, account := range resp.TokenAccounts {
		var found bool
		for _, existing := range accounts[1:] {
			if bytes.Equal(account.Value, existing) {
				found = true
				break
			}
		}
		assert.True(t, found)
	}
	assert.EqualValues(t, accounts[1], resp.TokenAccounts[0].Value)
	for i := 2; i < len(resp.TokenAccounts); i++ {
		assert.True(t, bytes.Compare(resp.TokenAccounts[i-1].Value, resp.TokenAccounts[i].Value) < 0)
	}
	for i := 0; i < len(resp.TokenAccountInfos); i++ {
		assert.Equal(t, resp.TokenAccounts[i].Value, resp.TokenAccountInfos[i].AccountId.Value)
		assert.EqualValues(t, 0, resp.TokenAccountInfos[i].Balance)
		assert.Empty(t, resp.TokenAccountInfos[i].Owner)
		assert.Empty(t, resp.TokenAccountInfos[i].CloseAuthority)
	}

	resp, err = env.client.ResolveTokenAccounts(context.Background(), &accountpb.ResolveTokenAccountsRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: accounts[2],
		},
	})
	assert.NoError(t, err)
	assert.Empty(t, resp.TokenAccounts)

	resp, err = env.client.ResolveTokenAccounts(context.Background(), &accountpb.ResolveTokenAccountsRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: accountsWithAssoc[0],
		},
	})
	assert.NoError(t, err)
	for _, account := range resp.TokenAccounts {
		var found bool
		for _, existing := range accountsWithAssoc[1:] {
			if bytes.Equal(account.Value, existing) {
				found = true
				break
			}
		}
		assert.True(t, found)
	}
	assert.EqualValues(t, assocAddr, resp.TokenAccounts[0].Value)
	for i := 2; i < len(resp.TokenAccounts); i++ {
		assert.True(t, bytes.Compare(resp.TokenAccounts[i-1].Value, resp.TokenAccounts[i].Value) < 0)
	}
	for i := 0; i < len(resp.TokenAccountInfos); i++ {
		assert.Equal(t, resp.TokenAccounts[i].Value, resp.TokenAccountInfos[i].AccountId.Value)
		assert.EqualValues(t, 0, resp.TokenAccountInfos[i].Balance)
		assert.Empty(t, resp.TokenAccountInfos[i].Owner)
		assert.Empty(t, resp.TokenAccountInfos[i].CloseAuthority)
	}

	resp, err = env.client.ResolveTokenAccounts(context.Background(), &accountpb.ResolveTokenAccountsRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: accountsWithAssocAndIdentity[0],
		},
	})
	assert.NoError(t, err)
	for _, account := range resp.TokenAccounts {
		var found bool
		for _, existing := range accountsWithAssocAndIdentity[0:] {
			if bytes.Equal(account.Value, existing) {
				found = true
				break
			}
		}
		assert.True(t, found)
	}
	assert.EqualValues(t, assocAddrWithIdentity, resp.TokenAccounts[0].Value)
	assert.EqualValues(t, accountsWithAssocAndIdentity[0], resp.TokenAccounts[1].Value)
	for i := 3; i < len(resp.TokenAccounts); i++ {
		assert.True(t, bytes.Compare(resp.TokenAccounts[i-1].Value, resp.TokenAccounts[i].Value) < 0)
	}
	for i := 0; i < len(resp.TokenAccountInfos); i++ {
		assert.Equal(t, resp.TokenAccounts[i].Value, resp.TokenAccountInfos[i].AccountId.Value)
		assert.EqualValues(t, 0, resp.TokenAccountInfos[i].Balance)
		assert.Empty(t, resp.TokenAccountInfos[i].Owner)
		assert.Empty(t, resp.TokenAccountInfos[i].CloseAuthority)
	}
}

func TestResolveTokenAccounts_WithInfo(t *testing.T) {
	env, cleanup := setup(t, nil, WithOverrides(config.NoopConfig, memconfig.NewConfig(0.0)))
	defer cleanup()

	// note: we create a reverse() function to ensure that the impl
	//       does proper sorting on the results from the underlying client.
	owner := testutil.GenerateSolanaKeys(t, 1)[0]
	closeAuthority := testutil.GenerateSolanaKeys(t, 1)[0]
	accounts := testutil.GenerateSolanaKeys(t, 4)
	testutil.SortKeys(accounts)

	// Accounts[0] -> set of accounts
	env.sc.On("GetTokenAccountsByOwner", owner, env.token).Return(accounts, nil)
	for i := 0; i < len(accounts); i++ {
		tokenAccount := token.Account{
			State:          token.AccountStateInitialized,
			Mint:           env.token,
			Owner:          owner,
			CloseAuthority: closeAuthority,
			Amount:         1 + uint64(i),
		}
		accountInfo := solana.AccountInfo{
			Data:  tokenAccount.Marshal(),
			Owner: token.ProgramKey,
		}
		env.sc.On("GetAccountInfo", accounts[i], mock.Anything).Return(accountInfo, nil).Once()
	}

	resp, err := env.client.ResolveTokenAccounts(context.Background(), &accountpb.ResolveTokenAccountsRequest{
		IncludeAccountInfo: true,
		AccountId: &commonpb.SolanaAccountId{
			Value: owner,
		},
	})
	assert.NoError(t, err)
	for _, account := range resp.TokenAccounts {
		var found bool
		for _, existing := range accounts {
			if bytes.Equal(account.Value, existing) {
				found = true
				break
			}
		}
		assert.True(t, found)
	}
	for i := 1; i < len(resp.TokenAccounts); i++ {
		assert.True(t, bytes.Compare(resp.TokenAccounts[i-1].Value, resp.TokenAccounts[i].Value) < 0)
	}

	for i := 0; i < len(resp.TokenAccountInfos); i++ {
		assert.Equal(t, resp.TokenAccounts[i].Value, resp.TokenAccountInfos[i].AccountId.Value)
		assert.EqualValues(t, i+1, resp.TokenAccountInfos[i].Balance)
		assert.EqualValues(t, owner, resp.TokenAccountInfos[i].Owner.Value)
		assert.EqualValues(t, closeAuthority, resp.TokenAccountInfos[i].CloseAuthority.Value)
	}
}

func TestResolveTokenAccounts_WithInfo_NotFound(t *testing.T) {
	env, cleanup := setup(t, nil, WithOverrides(config.NoopConfig, memconfig.NewConfig(0.0)))
	defer cleanup()

	// note: we create a reverse() function to ensure that the impl
	//       does proper sorting on the results from the underlying client.
	owner := testutil.GenerateSolanaKeys(t, 1)[0]
	accounts := testutil.GenerateSolanaKeys(t, 4)
	testutil.SortKeys(accounts)

	// Accounts[0] -> set of accounts
	env.sc.On("GetTokenAccountsByOwner", owner, env.token).Return(accounts, nil)
	for i := 0; i < len(accounts); i++ {
		env.sc.On("GetAccountInfo", accounts[i], mock.Anything).Return(solana.AccountInfo{}, solana.ErrNoAccountInfo).Once()
	}

	_, err := env.client.ResolveTokenAccounts(context.Background(), &accountpb.ResolveTokenAccountsRequest{
		IncludeAccountInfo: true,
		AccountId: &commonpb.SolanaAccountId{
			Value: owner,
		},
	})
	assert.Equal(t, codes.Internal, status.Code(err))
}

func TestResolveTokenAccounts_Cached(t *testing.T) {
	env, cleanup := setup(t, nil, WithOverrides(config.NoopConfig, memconfig.NewConfig(0.0)))
	defer cleanup()

	accounts := testutil.GenerateSolanaKeys(t, 5)
	reverse := func(keys []ed25519.PublicKey) []ed25519.PublicKey {
		r := make([]ed25519.PublicKey, len(keys))
		for i := range keys {
			r[len(keys)-1-i] = keys[i]
		}
		return r
	}

	// token accounts for accounts[0] should get cached, so this should get called only once
	env.sc.On("GetTokenAccountsByOwner", accounts[0], env.token).Return(reverse(accounts[1:]), nil).Times(1)

	resp, err := env.client.ResolveTokenAccounts(context.Background(), &accountpb.ResolveTokenAccountsRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: accounts[0],
		},
	})
	require.NoError(t, err)
	for _, account := range resp.TokenAccounts {
		var found bool
		for _, existing := range accounts[1:] {
			if bytes.Equal(account.Value, existing) {
				found = true
				break
			}
		}
		assert.True(t, found)
	}
	for i := 0; i < len(resp.TokenAccountInfos); i++ {
		assert.Equal(t, resp.TokenAccounts[i].Value, resp.TokenAccountInfos[i].AccountId.Value)
		assert.EqualValues(t, 0, resp.TokenAccountInfos[i].Balance)
		assert.Empty(t, resp.TokenAccountInfos[i].Owner)
		assert.Empty(t, resp.TokenAccountInfos[i].CloseAuthority)
	}

	secondResp, err := env.client.ResolveTokenAccounts(context.Background(), &accountpb.ResolveTokenAccountsRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: accounts[0],
		},
	})
	require.NoError(t, err)
	assert.True(t, proto.Equal(resp, secondResp))

	// no token accounts gets returned for accounts[1], so this should get called twice
	env.sc.On("GetTokenAccountsByOwner", accounts[1], env.token).Return([]ed25519.PublicKey{}, nil).Times(2)

	resp, err = env.client.ResolveTokenAccounts(context.Background(), &accountpb.ResolveTokenAccountsRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: accounts[1],
		},
	})
	require.NoError(t, err)
	assert.Empty(t, resp.TokenAccounts)

	resp, err = env.client.ResolveTokenAccounts(context.Background(), &accountpb.ResolveTokenAccountsRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: accounts[1],
		},
	})
	require.NoError(t, err)
	assert.Empty(t, resp.TokenAccounts)

	env.sc.AssertExpectations(t)
}

func TestResolveTokenAccounts_CacheCheck(t *testing.T) {
	// Always check
	env, cleanup := setup(t, nil, WithOverrides(config.NoopConfig, memconfig.NewConfig(1.0)))
	defer cleanup()

	accounts := testutil.GenerateSolanaKeys(t, 5)
	reverse := func(keys []ed25519.PublicKey) []ed25519.PublicKey {
		r := make([]ed25519.PublicKey, len(keys))
		for i := range keys {
			r[len(keys)-1-i] = keys[i]
		}
		return r
	}

	// token accounts for accounts[0] should get cached, but check freq is 1.0, so this should get called twice
	env.sc.On("GetTokenAccountsByOwner", accounts[0], env.token).Return(reverse(accounts[1:]), nil).Times(2)

	resp, err := env.client.ResolveTokenAccounts(context.Background(), &accountpb.ResolveTokenAccountsRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: accounts[0],
		},
	})
	require.NoError(t, err)
	for _, account := range resp.TokenAccounts {
		var found bool
		for _, existing := range accounts[1:] {
			if bytes.Equal(account.Value, existing) {
				found = true
				break
			}
		}
		assert.True(t, found)
	}
	for i := 0; i < len(resp.TokenAccountInfos); i++ {
		assert.Equal(t, resp.TokenAccounts[i].Value, resp.TokenAccountInfos[i].AccountId.Value)
		assert.EqualValues(t, 0, resp.TokenAccountInfos[i].Balance)
		assert.Empty(t, resp.TokenAccountInfos[i].Owner)
		assert.Empty(t, resp.TokenAccountInfos[i].CloseAuthority)
	}

	cached, err := env.tokenAccountCache.Get(context.Background(), accounts[0])
	require.NoError(t, err)
	assert.True(t, len(cached) > 0)

	secondResp, err := env.client.ResolveTokenAccounts(context.Background(), &accountpb.ResolveTokenAccountsRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: accounts[0],
		},
	})
	require.NoError(t, err)
	assert.True(t, proto.Equal(resp, secondResp))

	env.sc.AssertExpectations(t)
}

func TestResolveTokenAccounts_NoShortcuts(t *testing.T) {
	mig := &mockMigrator{}
	env, cleanup := setup(t, mig, WithOverrides(memconfig.NewConfig(false), memconfig.NewConfig(0.0)))
	defer cleanup()

	// Horizon client will be...called?
	migratable := testutil.GenerateSolanaKeys(t, 3)
	nonMigratable := testutil.GenerateSolanaKeys(t, 3)

	mig.On("InitiateMigration", mock.Anything, migratable[0], false, solana.CommitmentRecent).Return(nil)

	mig.On("GetMigrationAccounts", mock.Anything, migratable[0]).Return(migratable[1:2], nil)
	mig.On("GetMigrationAccounts", mock.Anything, nonMigratable[0]).Return([]ed25519.PublicKey{}, migration.ErrNotFound)
	env.sc.On("GetTokenAccountsByOwner", migratable[0], env.token).Return(migratable[1:], nil).Times(1)
	env.sc.On("GetTokenAccountsByOwner", nonMigratable[0], env.token).Return(nonMigratable[1:], nil).Times(1)

	resp, err := env.client.ResolveTokenAccounts(context.Background(), &accountpb.ResolveTokenAccountsRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: migratable[0],
		},
	})
	assert.NoError(t, err)
	assert.Len(t, resp.TokenAccounts, 2)
	assert.Contains(t, migratable[1:], ed25519.PublicKey(resp.TokenAccounts[0].Value))
	assert.Contains(t, migratable[1:], ed25519.PublicKey(resp.TokenAccounts[0].Value))

	resp, err = env.client.ResolveTokenAccounts(context.Background(), &accountpb.ResolveTokenAccountsRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: nonMigratable[0],
		},
	})
	assert.NoError(t, err)
	assert.Len(t, resp.TokenAccounts, 2)
	assert.Contains(t, nonMigratable[1:], ed25519.PublicKey(resp.TokenAccounts[0].Value))
	assert.Contains(t, nonMigratable[1:], ed25519.PublicKey(resp.TokenAccounts[1].Value))
}

func TestGetEvents(t *testing.T) {
	env, cleanup := setup(t, nil, WithOverrides(config.NoopConfig, memconfig.NewConfig(0.0)))
	defer cleanup()

	accounts := testutil.GenerateSolanaKeys(t, 4)
	tokenAccount := token.Account{
		Mint:   env.token,
		Amount: 10,
	}
	accountInfo := solana.AccountInfo{
		Data:  tokenAccount.Marshal(),
		Owner: token.ProgramKey,
	}
	env.sc.On("GetAccountInfo", mock.Anything, mock.Anything).Return(accountInfo, nil)

	stream, err := env.client.GetEvents(context.Background(), &accountpb.GetEventsRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: accounts[0],
		},
	})
	assert.NoError(t, err)

	resp, err := stream.Recv()
	assert.NoError(t, err)

	// initial state
	assert.Equal(t, accountpb.Events_OK, resp.Result)
	assert.Len(t, resp.Events, 1)
	assert.EqualValues(t, accounts[0], resp.Events[0].GetAccountUpdateEvent().AccountInfo.AccountId.Value)
	assert.EqualValues(t, 10, resp.Events[0].GetAccountUpdateEvent().AccountInfo.Balance)

	txn := solana.NewTransaction(
		env.subsidizer.Public().(ed25519.PublicKey),
		token.Transfer(
			accounts[0],
			accounts[1],
			accounts[0],
			5,
		),
	)
	env.notifier.OnTransaction(solana.BlockTransaction{Transaction: txn})

	// Relevant Transaction update
	resp, err = stream.Recv()
	assert.NoError(t, err)
	assert.Equal(t, accountpb.Events_OK, resp.Result)
	assert.EqualValues(t, txn.Marshal(), resp.Events[0].GetTransactionEvent().Transaction.Value)

	txn = solana.NewTransaction(
		env.subsidizer.Public().(ed25519.PublicKey),
		token.Transfer(
			accounts[2],
			accounts[3],
			accounts[2],
			5,
		),
	)
	env.notifier.OnTransaction(solana.BlockTransaction{Transaction: txn})

	// Irrelevant update
	received := make(chan struct{}, 1)
	go func() {
		_, err := stream.Recv()
		assert.NotNil(t, err)
		received <- struct{}{}
	}()

	select {
	case <-received:
		t.Fatal("received irrelevant notification")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestGetEvents_NotFound(t *testing.T) {
	env, cleanup := setup(t, nil, WithOverrides(config.NoopConfig, memconfig.NewConfig(0.0)))
	defer cleanup()

	account := testutil.GenerateSolanaKeys(t, 1)[0]

	env.sc.On("GetAccountInfo", mock.Anything, mock.Anything).Return(solana.AccountInfo{}, solana.ErrNoAccountInfo)
	env.sc.On("GetTokenAccountsByOwner", mock.Anything, env.token).Return(make([]ed25519.PublicKey, 0), nil)
	stream, err := env.client.GetEvents(context.Background(), &accountpb.GetEventsRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: account,
		},
	})
	assert.NoError(t, err)

	resp, err := stream.Recv()
	assert.NoError(t, err)
	assert.Equal(t, accountpb.Events_NOT_FOUND, resp.Result)
}

func TestGetEvents_WithResolution(t *testing.T) {
	env, cleanup := setup(t, nil, WithOverrides(config.NoopConfig, memconfig.NewConfig(0.0)))
	defer cleanup()

	accounts := testutil.GenerateSolanaKeys(t, 3)
	ownerID := accounts[0]
	tokenAccountID := accounts[1]

	tokenAccount := token.Account{
		Mint:   env.token,
		Amount: 10,
	}
	accountInfo := solana.AccountInfo{
		Data:  tokenAccount.Marshal(),
		Owner: token.ProgramKey,
	}
	env.sc.On("GetAccountInfo", ownerID, mock.Anything).Return(solana.AccountInfo{}, solana.ErrNoAccountInfo)
	env.sc.On("GetTokenAccountsByOwner", ownerID, env.token).Return([]ed25519.PublicKey{tokenAccountID}, nil)
	env.sc.On("GetAccountInfo", tokenAccountID, mock.Anything).Return(accountInfo, nil)

	stream, err := env.client.GetEvents(context.Background(), &accountpb.GetEventsRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: ownerID,
		},
	})
	assert.NoError(t, err)

	resp, err := stream.Recv()
	assert.NoError(t, err)

	assert.Equal(t, accountpb.Events_OK, resp.Result)
	assert.Len(t, resp.Events, 1)
	assert.EqualValues(t, ownerID, resp.Events[0].GetAccountUpdateEvent().AccountInfo.AccountId.Value)
	assert.EqualValues(t, 10, resp.Events[0].GetAccountUpdateEvent().AccountInfo.Balance)

	txn := solana.NewTransaction(
		env.subsidizer.Public().(ed25519.PublicKey),
		token.Transfer(
			tokenAccountID,
			accounts[2],
			ownerID,
			5,
		),
	)
	env.notifier.OnTransaction(solana.BlockTransaction{Transaction: txn})

	resp, err = stream.Recv()
	assert.NoError(t, err)
	assert.Equal(t, accountpb.Events_OK, resp.Result)
	assert.EqualValues(t, txn.Marshal(), resp.Events[0].GetTransactionEvent().Transaction.Value)
}

func TestGetEvents_WithCacheResolution(t *testing.T) {
	env, cleanup := setup(t, nil, WithOverrides(config.NoopConfig, memconfig.NewConfig(0.0)))
	defer cleanup()

	accounts := testutil.GenerateSolanaKeys(t, 3)
	ownerID := accounts[0]
	tokenAccountID := accounts[1]
	tokenAccount := token.Account{
		Mint:   env.token,
		Amount: 10,
	}
	accountInfo := solana.AccountInfo{
		Data:  tokenAccount.Marshal(),
		Owner: token.ProgramKey,
	}
	env.sc.On("GetAccountInfo", ownerID, mock.Anything).Return(solana.AccountInfo{}, solana.ErrNoAccountInfo)
	require.NoError(t, env.tokenAccountCache.Put(context.Background(), ownerID, []ed25519.PublicKey{tokenAccountID}))
	env.sc.On("GetAccountInfo", tokenAccountID, mock.Anything).Return(accountInfo, nil)

	stream, err := env.client.GetEvents(context.Background(), &accountpb.GetEventsRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: ownerID,
		},
	})
	assert.NoError(t, err)

	resp, err := stream.Recv()
	assert.NoError(t, err)

	assert.Equal(t, accountpb.Events_OK, resp.Result)
	assert.Len(t, resp.Events, 1)
	assert.EqualValues(t, ownerID, resp.Events[0].GetAccountUpdateEvent().AccountInfo.AccountId.Value)
	assert.EqualValues(t, 10, resp.Events[0].GetAccountUpdateEvent().AccountInfo.Balance)

	txn := solana.NewTransaction(
		env.subsidizer.Public().(ed25519.PublicKey),
		token.Transfer(
			tokenAccountID,
			accounts[2],
			ownerID,
			5,
		),
	)
	env.notifier.OnTransaction(solana.BlockTransaction{Transaction: txn})

	resp, err = stream.Recv()
	assert.NoError(t, err)
	assert.Equal(t, accountpb.Events_OK, resp.Result)
	assert.EqualValues(t, txn.Marshal(), resp.Events[0].GetTransactionEvent().Transaction.Value)
}

func TestGetEvents_WithResolutionTooManyAccounts(t *testing.T) {
	env, cleanup := setup(t, nil, WithOverrides(config.NoopConfig, memconfig.NewConfig(0.0)))
	defer cleanup()

	accounts := testutil.GenerateSolanaKeys(t, 3)
	ownerID := accounts[0]

	env.sc.On("GetAccountInfo", ownerID, mock.Anything).Return(solana.AccountInfo{}, solana.ErrNoAccountInfo)
	env.sc.On("GetTokenAccountsByOwner", ownerID, env.token).Return([]ed25519.PublicKey{accounts[1], accounts[2]}, nil)
	stream, err := env.client.GetEvents(context.Background(), &accountpb.GetEventsRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: ownerID,
		},
	})
	assert.NoError(t, err)

	resp, err := stream.Recv()
	assert.NoError(t, err)
	assert.Equal(t, accountpb.Events_NOT_FOUND, resp.Result)
}

type mockMigrator struct {
	sync.Mutex
	mock.Mock
}

func (m *mockMigrator) InitiateMigration(ctx context.Context, account ed25519.PublicKey, ignoreBalance bool, commitment solana.Commitment) error {
	m.Lock()
	defer m.Unlock()

	args := m.Called(ctx, account, ignoreBalance, commitment)
	return args.Error(0)
}

func (m *mockMigrator) GetMigrationAccounts(ctx context.Context, account ed25519.PublicKey) ([]ed25519.PublicKey, error) {
	m.Lock()
	defer m.Unlock()

	args := m.Called(ctx, account)
	return args.Get(0).([]ed25519.PublicKey), args.Error(1)
}
