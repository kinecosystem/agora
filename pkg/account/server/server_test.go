package server

import (
	"context"
	"os"
	"testing"

	"github.com/kinecosystem/agora-common/testutil"
	"github.com/kinecosystem/go/clients/horizon"
	"github.com/kinecosystem/go/keypair"
	horizonprotocols "github.com/kinecosystem/go/protocols/horizon"
	"github.com/kinecosystem/go/protocols/horizon/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	accountpb "github.com/kinecosystem/kin-api-internal/genproto/account/v3"
	commonpb "github.com/kinecosystem/kin-api-internal/genproto/common/v3"
)

type testEnv struct {
	client        accountpb.AccountClient
	horizonClient *horizon.MockClient
	rootAccountKP *keypair.Full
}

func setup(t *testing.T) (env testEnv, cleanup func()) {
	err := os.Setenv("AGORA_ENVIRONMENT", "test")
	require.NoError(t, err)

	conn, serv, err := testutil.NewServer()
	require.NoError(t, err)

	env.client = accountpb.NewAccountClient(conn)
	env.horizonClient = &horizon.MockClient{}

	kp, err := keypair.Random()
	require.NoError(t, err)
	env.rootAccountKP = kp

	s := New(env.rootAccountKP, env.horizonClient)
	serv.RegisterService(func(server *grpc.Server) {
		accountpb.RegisterAccountServer(server, s)
	})

	cleanup, err = serv.Serve()
	require.NoError(t, err)

	return env, cleanup
}

func TestCreateAccount(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	kp, err := keypair.Random()
	require.NoError(t, err)

	horizonErr := &horizon.Error{Problem: horizon.Problem{Status: 404}}
	env.horizonClient.On("LoadAccount", kp.Address()).Return(horizonprotocols.Account{}, horizonErr).Once()
	env.horizonClient.On("LoadAccount", env.rootAccountKP.Address()).Return(horizonprotocols.Account{Sequence: "1"}, nil).Once()
	env.horizonClient.On("SubmitTransaction", mock.AnythingOfType("string")).Return(horizonprotocols.TransactionSuccess{}, nil).Once()
	env.horizonClient.On("LoadAccount", kp.Address()).Return(*generateHorizonAccount("100", "1"), nil).Once()

	req := accountpb.CreateAccountRequest{
		AccountId: &commonpb.StellarAccountId{Value: kp.Address()},
		AppMapping: &accountpb.CreateAccountRequest_AppUserMapping{
			AppId:        "kin",
			AppAccountId: "someuser",
		},
	}
	resp, err := env.client.CreateAccount(context.Background(), &req)
	require.NoError(t, err)

	assert.Equal(t, accountpb.CreateAccountResponse_OK, resp.GetResult())
	assert.Equal(t, kp.Address(), resp.GetAccountInfo().GetAccountId().Value)
	assert.Equal(t, int64(100*100000), resp.GetAccountInfo().GetBalance())
	assert.Equal(t, int64(1), resp.GetAccountInfo().GetSequenceNumber())

	env.horizonClient.AssertExpectations(t)
}

func TestCreateAccountExists(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	kp, err := keypair.Random()
	require.NoError(t, err)

	env.horizonClient.On("LoadAccount", kp.Address()).Return(*generateHorizonAccount("100", "1"), nil).Once()

	req := accountpb.CreateAccountRequest{AccountId: &commonpb.StellarAccountId{Value: kp.Address()}}

	resp, err := env.client.CreateAccount(context.Background(), &req)
	require.NoError(t, err)

	assert.Equal(t, accountpb.CreateAccountResponse_EXISTS, resp.GetResult())
	assert.Equal(t, kp.Address(), resp.GetAccountInfo().GetAccountId().Value)
	assert.Equal(t, int64(100*100000), resp.GetAccountInfo().GetBalance())
	assert.Equal(t, int64(1), resp.GetAccountInfo().GetSequenceNumber())

	env.horizonClient.AssertExpectations(t)
}

func TestGetAccountInfo(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	kp, err := keypair.Random()
	require.NoError(t, err)

	env.horizonClient.On("LoadAccount", kp.Address()).Return(*generateHorizonAccount("100", "1"), nil).Once()

	req := accountpb.GetAccountInfoRequest{AccountId: &commonpb.StellarAccountId{Value: kp.Address()}}
	resp, err := env.client.GetAccountInfo(context.Background(), &req)
	require.NoError(t, err)

	assert.Equal(t, accountpb.GetAccountInfoResponse_OK, resp.Result)
	assert.Equal(t, kp.Address(), resp.GetAccountInfo().GetAccountId().Value)
	assert.Equal(t, int64(100*100000), resp.GetAccountInfo().GetBalance())
	assert.Equal(t, int64(1), resp.GetAccountInfo().GetSequenceNumber())

	env.horizonClient.AssertExpectations(t)
}

func TestGetAccountInfoNotFound(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	kp, err := keypair.Random()
	require.NoError(t, err)

	horizonErr := &horizon.Error{Problem: horizon.Problem{Status: 404}}
	env.horizonClient.On("LoadAccount", kp.Address()).Return(horizonprotocols.Account{}, horizonErr).Once()

	req := accountpb.GetAccountInfoRequest{AccountId: &commonpb.StellarAccountId{Value: kp.Address()}}
	resp, err := env.client.GetAccountInfo(context.Background(), &req)
	require.NoError(t, err)

	assert.Equal(t, accountpb.GetAccountInfoResponse_NOT_FOUND, resp.Result)

	env.horizonClient.AssertExpectations(t)
}

func generateHorizonAccount(nativeBalance string, sequence string) *horizonprotocols.Account {
	return &horizonprotocols.Account{
		Balances: []horizonprotocols.Balance{{
			Balance: nativeBalance,
			Asset:   base.Asset{Type: "native"},
		}},
		Sequence: sequence,
	}
}
