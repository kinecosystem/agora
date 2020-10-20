package server

import (
	"context"
	"crypto/ed25519"
	"net"
	"testing"

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

	airdroppb "github.com/kinecosystem/agora-api/genproto/airdrop/v4"
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v4"
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
}

func setup(t *testing.T) (*testEnv, func()) {
	token, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	subsidizer, subsidizerKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	env := &testEnv{
		token:         token,
		subsidizer:    subsidizer,
		subsidizerKey: subsidizerKey,
		sClient:       solana.NewMockClient(),
	}

	env.sClient.On("GetMinimumBalanceForRentExemption", mock.Anything).Return(accountLamports, nil).Once()

	l := bufconn.Listen(1)
	service := New(env.sClient, token, subsidizer, subsidizerKey, subsidizerKey)

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

	env.sClient.On("GetAccountInfo", mock.AnythingOfType("PublicKey"), mock.AnythingOfType("Commitment")).
		Return(accountInfo, nil).
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
}
