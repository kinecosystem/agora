package server

import (
	"context"
	"crypto/ed25519"
	"net"
	"testing"
	"time"

	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v4"
	airdroppb "github.com/kinecosystem/agora-api/genproto/airdrop/v4"
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v4"

	"github.com/kinecosystem/agora/pkg/account/info"
	infomemory "github.com/kinecosystem/agora/pkg/account/info/memory"
	"github.com/kinecosystem/agora/pkg/account/specstate"
)

const (
	accountLamports = 10
)

type testEnv struct {
	token         ed25519.PublicKey
	subsidizer    ed25519.PublicKey
	subsidizerKey ed25519.PrivateKey
	sClient       *solana.MockClient
	client        airdroppb.AirdropClient
	infoCache     info.Cache
}

func setup(t *testing.T) (*testEnv, func()) {
	tokenAccount, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	subsidizer, subsidizerKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	env := &testEnv{
		token:         tokenAccount,
		subsidizer:    subsidizer,
		subsidizerKey: subsidizerKey,
		sClient:       solana.NewMockClient(),
	}
	env.infoCache, err = infomemory.NewCache(5*time.Second, 5*time.Second, 1000)
	require.NoError(t, err)

	env.sClient.On("GetMinimumBalanceForRentExemption", mock.Anything).Return(accountLamports, nil).Once()

	l := bufconn.Listen(1)
	service := New(
		env.sClient,
		tokenAccount,
		subsidizer,
		subsidizerKey,
		subsidizerKey,
		specstate.NewSpeculativeLoader(token.NewClient(env.sClient, env.token), env.infoCache),
	)

	s := grpc.NewServer()
	airdroppb.RegisterAirdropServer(s, service)
	go func() {
		if err := s.Serve(l); err != nil {
			logrus.WithError(err).Warn("serve did not shut down gracefully")
		}
	}()

	dialer := func(context.Context, string) (net.Conn, error) {
		return l.Dial()
	}
	cc, err := grpc.Dial("", grpc.WithContextDialer(dialer), grpc.WithInsecure())
	require.NoError(t, err)

	env.client = airdroppb.NewAirdropClient(cc)

	return env, s.Stop
}

func TestAirdrop(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	account, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	var blockHash solana.Blockhash
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

	expected := solana.NewTransaction(
		env.subsidizer,
		token.Transfer(
			env.subsidizer,
			account,
			env.subsidizer,
			20,
		),
	)
	expected.SetBlockhash(blockHash)
	assert.NoError(t, expected.Sign(env.subsidizerKey))

	env.sClient.On("GetAccountInfo", mock.AnythingOfType("PublicKey"), mock.AnythingOfType("Commitment")).
		Return(accountInfo, nil).
		Once()
	env.sClient.On("GetRecentBlockhash").Return(blockHash, nil).Once()

	// Cache balance info
	require.NoError(t, env.infoCache.Put(context.Background(), &accountpb.AccountInfo{
		AccountId: &commonpb.SolanaAccountId{Value: account},
		Balance:   10,
	}))

	var submitted solana.Transaction
	var commitment solana.Commitment

	var sig solana.Signature
	copy(sig[:], expected.Signature())
	env.sClient.On("SubmitTransaction", mock.AnythingOfType("Transaction"), mock.AnythingOfType("Commitment")).
		Return(sig, &solana.SignatureStatus{}, nil).
		Run(func(args mock.Arguments) {
			submitted = args.Get(0).(solana.Transaction)
			commitment = args.Get(1).(solana.Commitment)
		}).
		Once()

	resp, err := env.client.RequestAirdrop(context.Background(), &airdroppb.RequestAirdropRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: account,
		},
		Commitment: commonpb.Commitment_ROOT,
		Quarks:     20,
	})
	assert.NoError(t, err)
	assert.Equal(t, solana.CommitmentRoot, commitment)
	assert.Equal(t, airdroppb.RequestAirdropResponse_OK, resp.Result)

	assert.Equal(t, expected, submitted)
	assert.Equal(t, resp.Signature.Value, expected.Signature())

	// Check that cached balance info was updated
	ai, err := env.infoCache.Get(context.Background(), account)
	require.NoError(t, err)
	assert.EqualValues(t, 30, ai.Balance)
}

func TestAirdrop_NoFunds(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	account, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	var blockHash solana.Blockhash
	sourceAccount := token.Account{
		Mint:   env.token,
		Owner:  account,
		Amount: 0,
		State:  token.AccountStateInitialized,
	}
	accountInfo := solana.AccountInfo{
		Owner: token.ProgramKey,
		Data:  sourceAccount.Marshal(),
	}

	env.sClient.On("GetAccountInfo", mock.AnythingOfType("PublicKey"), mock.AnythingOfType("Commitment")).
		Return(accountInfo, nil)
	env.sClient.On("GetRecentHash").Return(blockHash, nil).Once()

	resp, err := env.client.RequestAirdrop(context.Background(), &airdroppb.RequestAirdropRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: account,
		},
		Commitment: commonpb.Commitment_ROOT,
		Quarks:     20,
	})
	assert.NoError(t, err)
	assert.Equal(t, airdroppb.RequestAirdropResponse_INSUFFICIENT_KIN, resp.Result)

	sourceAccount.Amount = 10 * maxAirdrop
	// The mock call will use the updated value below
	// nolint:ineffassign
	accountInfo = solana.AccountInfo{
		Owner: token.ProgramKey,
		Data:  sourceAccount.Marshal(),
	}

	var sig solana.Signature
	txErr, err := solana.TransactionErrorFromInstructionError(&solana.InstructionError{
		Index: 0,
		Err:   token.ErrorInsufficientFunds,
	})
	require.NoError(t, err)
	sigStatus := &solana.SignatureStatus{
		ErrorResult: txErr,
	}

	env.sClient.On("SubmitTransaction", mock.AnythingOfType("Transaction"), mock.AnythingOfType("Commitment")).
		Return(sig, sigStatus, nil).
		Once()

	resp, err = env.client.RequestAirdrop(context.Background(), &airdroppb.RequestAirdropRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: account,
		},
		Commitment: commonpb.Commitment_ROOT,
		Quarks:     20,
	})
	assert.NoError(t, err)
	assert.Equal(t, airdroppb.RequestAirdropResponse_INSUFFICIENT_KIN, resp.Result)
}

func TestAirdrop_TooLarge(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	account, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	_, err = env.client.RequestAirdrop(context.Background(), &airdroppb.RequestAirdropRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: account,
		},
		Commitment: commonpb.Commitment_ROOT,
		Quarks:     maxAirdrop + 1,
	})
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
}

func TestAirdrop_NotFound(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	account, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	var blockHash solana.Blockhash
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

	expected := solana.NewTransaction(
		env.subsidizer,
		token.Transfer(
			env.subsidizer,
			account,
			env.subsidizer,
			20,
		),
	)
	expected.SetBlockhash(blockHash)
	assert.NoError(t, expected.Sign(env.subsidizerKey))

	env.sClient.On("GetAccountInfo", env.subsidizer, mock.AnythingOfType("Commitment")).
		Return(accountInfo, nil).
		Once()
	env.sClient.On("GetAccountInfo", account, mock.AnythingOfType("Commitment")).
		Return(solana.AccountInfo{}, token.ErrAccountNotFound).
		Once()
	env.sClient.On("GetRecentBlockhash").Return(blockHash, nil).Once()

	var sig solana.Signature
	sigStatus := &solana.SignatureStatus{
		ErrorResult: solana.NewTransactionError(solana.TransactionErrorAccountNotFound),
	}

	copy(sig[:], expected.Signature())
	env.sClient.On("SubmitTransaction", mock.AnythingOfType("Transaction"), mock.AnythingOfType("Commitment")).
		Return(sig, sigStatus, nil).
		Once()

	resp, err := env.client.RequestAirdrop(context.Background(), &airdroppb.RequestAirdropRequest{
		AccountId: &commonpb.SolanaAccountId{
			Value: account,
		},
		Commitment: commonpb.Commitment_ROOT,
		Quarks:     20,
	})
	assert.NoError(t, err)
	assert.Equal(t, airdroppb.RequestAirdropResponse_NOT_FOUND, resp.Result)

	// Verify nothing was added to cache
	cached, err := env.infoCache.Get(context.Background(), account)
	assert.Equal(t, info.ErrAccountInfoNotFound, err)
	assert.Nil(t, cached)
}
