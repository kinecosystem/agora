package solana

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/agora-common/headers"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/system"
	"github.com/kinecosystem/agora-common/solana/token"
	agoratestutil "github.com/kinecosystem/agora-common/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	xrate "golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v4"
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v4"

	"github.com/kinecosystem/agora/pkg/account"
	memorymapper "github.com/kinecosystem/agora/pkg/account/memory"
	"github.com/kinecosystem/agora/pkg/account/solana/accountinfo"
	infodb "github.com/kinecosystem/agora/pkg/account/solana/accountinfo/memory"
	"github.com/kinecosystem/agora/pkg/account/solana/tokenaccount"
	"github.com/kinecosystem/agora/pkg/account/solana/tokenaccount/memory"
	"github.com/kinecosystem/agora/pkg/migration"
	migrationstore "github.com/kinecosystem/agora/pkg/migration/memory"
	"github.com/kinecosystem/agora/pkg/rate"
	"github.com/kinecosystem/agora/pkg/testutil"
)

type testEnv struct {
	token      ed25519.PublicKey
	subsidizer ed25519.PrivateKey
	client     accountpb.AccountClient
	mapper     account.Mapper

	minLamports uint64

	sc *solana.MockClient
	tc *token.Client

	notifier          *AccountNotifier
	tokenAccountCache tokenaccount.Cache
	infoCache         accountinfo.Cache
	migrationStore    migration.Store
	migrator          migration.Migrator

	server *server
}

func setup(t *testing.T, migrator migration.Migrator) (env testEnv, cleanup func()) {
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
	env.mapper = memorymapper.New()

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

	env.sc.On("GetMinimumBalanceForRentExemption", mock.Anything).Return(uint64(50), nil)
	env.minLamports = 50

	s, err := New(
		env.sc,
		account.NewLimiter(rate.NewLocalRateLimiter(xrate.Limit(5))),
		env.notifier,
		env.tokenAccountCache,
		env.infoCache,
		accountinfo.NewLoader(env.tc, env.infoCache, env.tokenAccountCache),
		migration.NewNoopLoader(),
		env.migrator,
		env.mapper,
		env.token,
		env.subsidizer,
		0.0,
		"",
	)
	require.NoError(t, err)
	env.server = s.(*server)

	serv.RegisterService(func(server *grpc.Server) {
		accountpb.RegisterAccountServer(server, s)
	})

	cleanup, err = serv.Serve()
	require.NoError(t, err)

	return env, cleanup
}

func TestCreateAccount(t *testing.T) {
	env, cleanup := setup(t, nil)
	defer cleanup()

	account := testutil.GenerateSolanaKeypair(t)
	owner := testutil.GenerateSolanaKeypair(t)

	createTxn := solana.NewTransaction(
		env.subsidizer.Public().(ed25519.PublicKey),
		system.CreateAccount(
			env.subsidizer.Public().(ed25519.PublicKey),
			account.Public().(ed25519.PublicKey),
			token.ProgramKey,
			env.minLamports,
			token.AccountSize,
		),
		token.InitializeAccount(
			account.Public().(ed25519.PublicKey),
			env.token,
			owner.Public().(ed25519.PublicKey),
		),
		token.SetAuthority(
			account.Public().(ed25519.PublicKey),
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

	resp, err := env.client.CreateAccount(context.Background(), &accountpb.CreateAccountRequest{
		Transaction: &commonpb.Transaction{
			Value: createTxn.Marshal(),
		},
		Commitment: commonpb.Commitment_MAX,
	})
	require.NoError(t, err)
	assert.Equal(t, accountpb.CreateAccountResponse_OK, resp.Result)

	mappedOwner, err := env.mapper.Get(context.Background(), account.Public().(ed25519.PublicKey), solana.CommitmentRecent)
	assert.NoError(t, err)
	assert.Equal(t, owner.Public().(ed25519.PublicKey), mappedOwner)

	cachedInfo, err := env.infoCache.Get(context.Background(), account.Public().(ed25519.PublicKey))
	assert.NoError(t, err)
	assert.EqualValues(t, account.Public().(ed25519.PublicKey), cachedInfo.AccountId.Value)
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
	env, cleanup := setup(t, nil)
	defer cleanup()

	account := testutil.GenerateSolanaKeypair(t)
	owner := testutil.GenerateSolanaKeypair(t)

	createTxn := solana.NewTransaction(
		env.subsidizer.Public().(ed25519.PublicKey),
		system.CreateAccount(
			env.subsidizer.Public().(ed25519.PublicKey),
			account.Public().(ed25519.PublicKey),
			token.ProgramKey,
			env.minLamports,
			token.AccountSize,
		),
		token.InitializeAccount(
			account.Public().(ed25519.PublicKey),
			env.token,
			owner.Public().(ed25519.PublicKey),
		),
		token.SetAuthority(
			account.Public().(ed25519.PublicKey),
			owner.Public().(ed25519.PublicKey),
			env.subsidizer.Public().(ed25519.PublicKey),
			token.AuthorityTypeCloseAccount,
		),
	)
	require.NoError(t, createTxn.Sign(env.subsidizer))

	err := env.server.loader.Update(context.Background(), owner.Public().(ed25519.PublicKey), &accountpb.AccountInfo{
		AccountId: &commonpb.SolanaAccountId{
			Value: account.Public().(ed25519.PublicKey),
		},
		Balance: 20,
	})
	require.NoError(t, err)

	var sig solana.Signature
	copy(sig[:], ed25519.Sign(env.subsidizer, createTxn.Marshal()))

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
	assert.EqualValues(t, account.Public().(ed25519.PublicKey), resp.AccountInfo.AccountId.Value)
	assert.EqualValues(t, 20, resp.AccountInfo.Balance)
}

func TestCreateAccount_NoSubsidizer(t *testing.T) {
	env, cleanup := setup(t, nil)
	defer cleanup()

	account := testutil.GenerateSolanaKeypair(t)
	owner := testutil.GenerateSolanaKeypair(t)
	env.server.subsidizer = nil

	createTxn := solana.NewTransaction(
		env.subsidizer.Public().(ed25519.PublicKey),
		system.CreateAccount(
			env.subsidizer.Public().(ed25519.PublicKey),
			account.Public().(ed25519.PublicKey),
			token.ProgramKey,
			env.minLamports,
			token.AccountSize,
		),
		token.InitializeAccount(
			account.Public().(ed25519.PublicKey),
			env.token,
			owner.Public().(ed25519.PublicKey),
		),
	)
	require.NoError(t, createTxn.Sign(env.subsidizer))

	var sig solana.Signature
	copy(sig[:], ed25519.Sign(env.subsidizer, createTxn.Marshal()))

	var submitted solana.Transaction
	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentMax).Return(sig, &solana.SignatureStatus{}, nil).Run(func(args mock.Arguments) {
		submitted = args.Get(0).(solana.Transaction)
	})

	resp, err := env.client.CreateAccount(context.Background(), &accountpb.CreateAccountRequest{
		Transaction: &commonpb.Transaction{
			Value: createTxn.Marshal(),
		},
		Commitment: commonpb.Commitment_MAX,
	})
	require.NoError(t, err)
	assert.Equal(t, accountpb.CreateAccountResponse_OK, resp.Result)

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

func TestCreateAccount_InvalidBlockhash(t *testing.T) {
	env, cleanup := setup(t, nil)
	defer cleanup()

	account := testutil.GenerateSolanaKeypair(t)
	owner := testutil.GenerateSolanaKeypair(t)
	env.server.subsidizer = nil

	createTxn := solana.NewTransaction(
		env.subsidizer.Public().(ed25519.PublicKey),
		system.CreateAccount(
			env.subsidizer.Public().(ed25519.PublicKey),
			account.Public().(ed25519.PublicKey),
			token.ProgramKey,
			env.minLamports,
			token.AccountSize,
		),
		token.InitializeAccount(
			account.Public().(ed25519.PublicKey),
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

	resp, err := env.client.CreateAccount(context.Background(), &accountpb.CreateAccountRequest{
		Transaction: &commonpb.Transaction{
			Value: createTxn.Marshal(),
		},
		Commitment: commonpb.Commitment_MAX,
	})
	require.NoError(t, err)
	assert.Equal(t, accountpb.CreateAccountResponse_BAD_NONCE, resp.Result)
}

func TestCreateAccount_Invalid(t *testing.T) {
	env, cleanup := setup(t, nil)
	defer cleanup()

	var txns []solana.Transaction

	accounts := testutil.GenerateSolanaKeys(t, 3)

	// Not a create
	txns = append(txns, solana.NewTransaction(
		env.subsidizer.Public().(ed25519.PublicKey),
		token.Transfer(
			accounts[0],
			accounts[1],
			accounts[2],
			10,
		),
		token.Transfer(
			accounts[0],
			accounts[1],
			accounts[2],
			10,
		),
	))

	// Partial create
	txns = append(txns, solana.NewTransaction(
		env.subsidizer.Public().(ed25519.PublicKey),
		system.CreateAccount(
			env.subsidizer.Public().(ed25519.PublicKey),
			accounts[0],
			token.ProgramKey,
			env.minLamports,
			token.AccountSize,
		),
	))

	// Create/Initialize mismatch
	txns = append(txns, solana.NewTransaction(
		env.subsidizer.Public().(ed25519.PublicKey),
		system.CreateAccount(
			env.subsidizer.Public().(ed25519.PublicKey),
			accounts[0],
			token.ProgramKey,
			env.minLamports,
			token.AccountSize,
		),
		token.InitializeAccount(
			accounts[1],
			env.token,
			accounts[2],
		),
	))

	// Create for other token
	txns = append(txns, solana.NewTransaction(
		env.subsidizer.Public().(ed25519.PublicKey),
		system.CreateAccount(
			env.subsidizer.Public().(ed25519.PublicKey),
			accounts[0],
			accounts[1],
			env.minLamports,
			token.AccountSize,
		),
		token.InitializeAccount(
			accounts[0],
			env.token,
			accounts[2],
		),
	))

	// Create without authority (and required)
	txns = append(txns, solana.NewTransaction(
		env.subsidizer.Public().(ed25519.PublicKey),
		system.CreateAccount(
			env.subsidizer.Public().(ed25519.PublicKey),
			accounts[0],
			token.ProgramKey,
			env.minLamports,
			token.AccountSize,
		),
		token.InitializeAccount(
			accounts[0],
			env.token,
			accounts[2],
		),
	))

	// Create with invalid authority
	txns = append(txns, solana.NewTransaction(
		env.subsidizer.Public().(ed25519.PublicKey),
		system.CreateAccount(
			env.subsidizer.Public().(ed25519.PublicKey),
			accounts[0],
			token.ProgramKey,
			env.minLamports,
			token.AccountSize,
		),
		token.InitializeAccount(
			accounts[0],
			env.token,
			accounts[2],
		),
		token.SetAuthority(
			accounts[0],
			accounts[0],
			accounts[0],
			token.AuthorityTypeCloseAccount,
		),
	))

	// Create with invalid set authority type
	txns = append(txns, solana.NewTransaction(
		env.subsidizer.Public().(ed25519.PublicKey),
		system.CreateAccount(
			env.subsidizer.Public().(ed25519.PublicKey),
			accounts[0],
			token.ProgramKey,
			env.minLamports,
			token.AccountSize,
		),
		token.InitializeAccount(
			accounts[0],
			env.token,
			accounts[2],
		),
		token.SetAuthority(
			accounts[0],
			accounts[0],
			env.subsidizer.Public().(ed25519.PublicKey),
			token.AuthorityTypeAccountHolder,
		),
	))

	// Create + others
	txns = append(txns, solana.NewTransaction(
		env.subsidizer.Public().(ed25519.PublicKey),
		system.CreateAccount(
			env.subsidizer.Public().(ed25519.PublicKey),
			accounts[0],
			token.ProgramKey,
			env.minLamports,
			token.AccountSize,
		),
		token.InitializeAccount(
			accounts[0],
			env.token,
			accounts[1],
		),
		token.Transfer(
			accounts[0],
			accounts[1],
			accounts[2],
			10,
		),
	))

	// Attempt to steal lamports
	txns = append(txns, solana.NewTransaction(
		env.subsidizer.Public().(ed25519.PublicKey),
		system.CreateAccount(
			env.subsidizer.Public().(ed25519.PublicKey),
			accounts[0],
			token.ProgramKey,
			env.minLamports*10,
			token.AccountSize,
		),
		token.InitializeAccount(
			accounts[0],
			env.token,
			accounts[1],
		),
	))

	// Invalid size
	txns = append(txns, solana.NewTransaction(
		env.subsidizer.Public().(ed25519.PublicKey),
		system.CreateAccount(
			env.subsidizer.Public().(ed25519.PublicKey),
			accounts[0],
			token.ProgramKey,
			env.minLamports,
			token.AccountSize/2,
		),
		token.InitializeAccount(
			accounts[0],
			env.token,
			accounts[1],
		),
	))

	for _, txn := range txns {
		_, err := env.client.CreateAccount(context.Background(), &accountpb.CreateAccountRequest{
			Transaction: &commonpb.Transaction{
				Value: txn.Marshal(),
			},
		})
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	}
}

func TestCreateAccount_Limited(t *testing.T) {
	env, cleanup := setup(t, nil)
	defer cleanup()

	account := testutil.GenerateSolanaKeypair(t)
	owner := testutil.GenerateSolanaKeypair(t)

	createTxn := solana.NewTransaction(
		env.subsidizer.Public().(ed25519.PublicKey),
		system.CreateAccount(
			env.subsidizer.Public().(ed25519.PublicKey),
			account.Public().(ed25519.PublicKey),
			token.ProgramKey,
			env.minLamports,
			token.AccountSize,
		),
		token.InitializeAccount(
			account.Public().(ed25519.PublicKey),
			env.token,
			owner.Public().(ed25519.PublicKey),
		),
		token.SetAuthority(
			account.Public().(ed25519.PublicKey),
			account.Public().(ed25519.PublicKey),
			env.subsidizer.Public().(ed25519.PublicKey),
			token.AuthorityTypeCloseAccount,
		),
	)
	require.NoError(t, createTxn.Sign(env.subsidizer))

	var sig solana.Signature
	copy(sig[:], ed25519.Sign(env.subsidizer, createTxn.Marshal()))

	env.sc.On("SubmitTransaction", mock.Anything, solana.CommitmentMax).Return(sig, &solana.SignatureStatus{}, nil)

	for i := 0; i < 5; i++ {
		_, err := env.client.CreateAccount(context.Background(), &accountpb.CreateAccountRequest{
			Transaction: &commonpb.Transaction{
				Value: createTxn.Marshal(),
			},
			Commitment: commonpb.Commitment_MAX,
		})
		require.NoError(t, err)
	}

	_, err := env.client.CreateAccount(context.Background(), &accountpb.CreateAccountRequest{
		Transaction: &commonpb.Transaction{
			Value: createTxn.Marshal(),
		},
		Commitment: commonpb.Commitment_MAX,
	})
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
}

func TestCreateAccount_Whitelisting(t *testing.T) {
	env, cleanup := setup(t, nil)
	defer cleanup()

	env.server.createWhitelistSecret = "somesecret"

	account := testutil.GenerateSolanaKeypair(t)
	owner := testutil.GenerateSolanaKeypair(t)

	createTxn := solana.NewTransaction(
		env.subsidizer.Public().(ed25519.PublicKey),
		system.CreateAccount(
			env.subsidizer.Public().(ed25519.PublicKey),
			account.Public().(ed25519.PublicKey),
			token.ProgramKey,
			env.minLamports,
			token.AccountSize,
		),
		token.InitializeAccount(
			account.Public().(ed25519.PublicKey),
			env.token,
			owner.Public().(ed25519.PublicKey),
		),
		token.SetAuthority(
			account.Public().(ed25519.PublicKey),
			owner.Public().(ed25519.PublicKey),
			env.subsidizer.Public().(ed25519.PublicKey),
			token.AuthorityTypeCloseAccount,
		),
	)
	require.NoError(t, createTxn.Sign(env.subsidizer))

	var sig solana.Signature
	copy(sig[:], ed25519.Sign(env.subsidizer, createTxn.Marshal()))

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
	env, cleanup := setup(t, nil)
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
	env, cleanup := setup(t, nil)
	defer cleanup()

	// note: we create a reverse() function to ensure that the impl
	//       does proper sorting on the results from the underlying client.
	accounts := testutil.GenerateSolanaKeys(t, 5)
	reverse := func(keys []ed25519.PublicKey) []ed25519.PublicKey {
		r := make([]ed25519.PublicKey, len(keys))
		for i := range keys {
			r[len(keys)-1-i] = keys[i]
		}
		return r
	}

	// Accounts[0] -> set of accounts, non-identity
	env.sc.On("GetTokenAccountsByOwner", accounts[0], env.token).Return(reverse(accounts[1:]), nil)
	// Accounts[1] -> set of accounts, _with_ identity
	env.sc.On("GetTokenAccountsByOwner", accounts[1], env.token).Return(reverse(accounts[1:]), nil)
	// Fallback
	env.sc.On("GetTokenAccountsByOwner", mock.Anything, env.token).Return([]ed25519.PublicKey{}, nil)
	// Existence check to see if the account requested exists
	for i := 0; i < 3; i++ {
		env.sc.On("GetAccountInfo", accounts[i], mock.Anything).Return(solana.AccountInfo{}, solana.ErrNoAccountInfo)
	}

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

	resp, err = env.client.ResolveTokenAccounts(context.Background(), &accountpb.ResolveTokenAccountsRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: accounts[2],
		},
	})
	assert.NoError(t, err)
	assert.Empty(t, resp.TokenAccounts)
}

func TestResolveTokenAccounts_Cached(t *testing.T) {
	env, cleanup := setup(t, nil)
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
	env, cleanup := setup(t, nil)
	defer cleanup()

	// Always check
	env.server.cacheCheckProbability = 1.0

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

func TestResolveTokenAccounts_MigrationSkip(t *testing.T) {
	mig := &mockMigrator{}
	env, cleanup := setup(t, mig)
	defer cleanup()

	// Horizon client will be...called?
	migrated := testutil.GenerateSolanaKeys(t, 2)
	notMigrated := testutil.GenerateSolanaKeys(t, 2)

	mig.On("InitiateMigration", mock.Anything, migrated[0], false, solana.CommitmentRecent).Return(nil)
	mig.On("InitiateMigration", mock.Anything, notMigrated[0], false, solana.CommitmentRecent).Return(nil)

	mig.On("GetMigrationAccounts", mock.Anything, migrated[0]).Return(migrated[1:2], nil)
	mig.On("GetMigrationAccounts", mock.Anything, notMigrated[0]).Return([]ed25519.PublicKey{}, migration.ErrNotFound)
	env.sc.On("GetAccountInfo", notMigrated[0], mock.Anything).Return(solana.AccountInfo{}, solana.ErrNoAccountInfo).Times(1)
	env.sc.On("GetTokenAccountsByOwner", notMigrated[0], env.token).Return(notMigrated[1:], nil).Times(1)

	resp, err := env.client.ResolveTokenAccounts(context.Background(), &accountpb.ResolveTokenAccountsRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: migrated[0],
		},
	})
	assert.NoError(t, err)
	assert.Len(t, resp.TokenAccounts, 1)
	assert.EqualValues(t, migrated[1], resp.TokenAccounts[0].Value)

	resp, err = env.client.ResolveTokenAccounts(context.Background(), &accountpb.ResolveTokenAccountsRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: notMigrated[0],
		},
	})
	assert.NoError(t, err)
	assert.Len(t, resp.TokenAccounts, 1)
	assert.EqualValues(t, notMigrated[1], resp.TokenAccounts[0].Value)
}

func TestResolveTokenAccounts_ReverseMap(t *testing.T) {
	mig := &mockMigrator{}
	env, cleanup := setup(t, mig)
	defer cleanup()

	kin3 := testutil.GenerateSolanaKeys(t, 1)[0]
	kin4 := testutil.GenerateSolanaKeys(t, 1)[0]
	misc := testutil.GenerateSolanaKeys(t, 1)[0]
	miscToken := testutil.GenerateSolanaKeys(t, 1)

	kin4Account := token.Account{
		Mint:   env.token,
		Owner:  kin3,
		Amount: 100,
		State:  token.AccountStateInitialized,
	}
	miscAccount := token.Account{
		Mint:   env.token,
		Owner:  misc,
		Amount: 100,
		State:  token.AccountStateInitialized,
	}
	accountInfos := []solana.AccountInfo{
		{
			Owner: token.ProgramKey,
			Data:  kin4Account.Marshal(),
		},
		{
			Owner: token.ProgramKey,
			Data:  miscAccount.Marshal(),
		},
	}

	assert.NoError(t, env.mapper.Add(context.Background(), kin4, kin3))

	// Kin3 migration hooks.
	mig.On("InitiateMigration", mock.Anything, kin3, false, solana.CommitmentRecent).Return(nil)
	mig.On("InitiateMigration", mock.Anything, misc, false, solana.CommitmentRecent).Return(migration.ErrNotFound)
	mig.On("GetMigrationAccounts", mock.Anything, kin3).Return([]ed25519.PublicKey{kin4}, nil)
	mig.On("GetMigrationAccounts", mock.Anything, kin4).Return([]ed25519.PublicKey{}, migration.ErrNotFound)
	mig.On("GetMigrationAccounts", mock.Anything, misc).Return([]ed25519.PublicKey{}, migration.ErrNotFound)
	mig.On("GetMigrationAccounts", mock.Anything, miscToken[0]).Return([]ed25519.PublicKey{}, migration.ErrNotFound)
	env.sc.On("GetAccountInfo", kin4Account, mock.Anything).Return(accountInfos[0], nil).Times(1)
	env.sc.On("GetAccountInfo", misc, mock.Anything).Return(accountInfos[1], nil).Times(1)
	env.sc.On("GetTokenAccountsByOwner", misc, env.token).Return(miscToken, nil).Times(1)
	env.sc.On("GetTokenAccountsByOwner", miscToken[0], env.token).Return([]ed25519.PublicKey{}, nil).Times(1)
	env.sc.On("GetTokenAccountsByOwner", kin4, env.token).Return([]ed25519.PublicKey{kin4}, nil).Times(1)

	// Resolving kin3 should work from here, no other calls
	resp, err := env.client.ResolveTokenAccounts(context.Background(), &accountpb.ResolveTokenAccountsRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: kin3,
		},
	})
	assert.NoError(t, err)
	assert.Len(t, resp.TokenAccounts, 1)
	assert.EqualValues(t, kin4, resp.TokenAccounts[0].Value)

	// Resolving kin4 should return itself.
	resp, err = env.client.ResolveTokenAccounts(context.Background(), &accountpb.ResolveTokenAccountsRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: kin4,
		},
	})
	assert.NoError(t, err)
	assert.Len(t, resp.TokenAccounts, 1)
	assert.EqualValues(t, kin4, resp.TokenAccounts[0].Value)

	// Resolving misc, and miscs token, by itself should be fine
	resp, err = env.client.ResolveTokenAccounts(context.Background(), &accountpb.ResolveTokenAccountsRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: misc,
		},
	})
	assert.NoError(t, err)
	assert.Len(t, resp.TokenAccounts, 1)
	assert.EqualValues(t, miscToken[0], resp.TokenAccounts[0].Value)

	resp, err = env.client.ResolveTokenAccounts(context.Background(), &accountpb.ResolveTokenAccountsRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: miscToken[0],
		},
	})
	assert.NoError(t, err)
	assert.Empty(t, resp.TokenAccounts)
}

func TestGetEvents(t *testing.T) {
	env, cleanup := setup(t, nil)
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
	env, cleanup := setup(t, nil)
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
	env, cleanup := setup(t, nil)
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
	env, cleanup := setup(t, nil)
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
	env, cleanup := setup(t, nil)
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
