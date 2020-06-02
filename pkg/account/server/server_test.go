package server

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/kinecosystem/agora-common/testutil"
	"github.com/kinecosystem/go/clients/horizon"
	"github.com/kinecosystem/go/keypair"
	hProtocol "github.com/kinecosystem/go/protocols/horizon"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v3"
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"

	"github.com/kinecosystem/agora/pkg/account/server/test"
)

type testEnv struct {
	client          accountpb.AccountClient
	horizonClient   *horizon.MockClient
	rootAccountKP   *keypair.Full
	accountNotifier *AccountNotifier
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

	env.accountNotifier = NewAccountNotifier(env.horizonClient)

	s := New(env.rootAccountKP, env.horizonClient, env.accountNotifier)
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
	env.horizonClient.On("LoadAccount", kp.Address()).Return(hProtocol.Account{}, horizonErr).Once()
	env.horizonClient.On("LoadAccount", env.rootAccountKP.Address()).Return(hProtocol.Account{Sequence: "1"}, nil).Once()
	env.horizonClient.On("SubmitTransaction", mock.AnythingOfType("string")).Return(hProtocol.TransactionSuccess{}, nil).Once()
	env.horizonClient.On("LoadAccount", kp.Address()).Return(*test.GenerateHorizonAccount(kp.Address(), "100", "1"), nil).Once()

	req := accountpb.CreateAccountRequest{
		AccountId: &commonpb.StellarAccountId{Value: kp.Address()},
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

	env.horizonClient.On("LoadAccount", kp.Address()).Return(*test.GenerateHorizonAccount(kp.Address(), "100", "1"), nil).Once()

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

	env.horizonClient.On("LoadAccount", kp.Address()).Return(*test.GenerateHorizonAccount(kp.Address(), "100", "1"), nil).Once()

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
	env.horizonClient.On("LoadAccount", kp.Address()).Return(hProtocol.Account{}, horizonErr).Once()

	req := accountpb.GetAccountInfoRequest{AccountId: &commonpb.StellarAccountId{Value: kp.Address()}}
	resp, err := env.client.GetAccountInfo(context.Background(), &req)
	require.NoError(t, err)

	assert.Equal(t, accountpb.GetAccountInfoResponse_NOT_FOUND, resp.Result)

	env.horizonClient.AssertExpectations(t)
}

func TestGetEvents_HappyPath(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	kp1, acc1 := test.GenerateAccountID(t)
	_, acc2 := test.GenerateAccountID(t)

	e := test.GenerateTransactionEnvelope(acc1, []xdr.Operation{test.GeneratePaymentOperation(nil, acc2)})
	m := test.GenerateTransactionMeta(0, []xdr.OperationMeta{
		{
			Changes: []xdr.LedgerEntryChange{
				test.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc1, 2, 900000),
				test.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc2, 2, 1100000),
			},
		},
	})

	env.horizonClient.On("LoadAccount", kp1.Address()).Return(*test.GenerateHorizonAccount(kp1.Address(), "10", "1"), nil).Once()

	req := &accountpb.GetEventsRequest{AccountId: &commonpb.StellarAccountId{Value: kp1.Address()}}
	stream, err := env.client.GetEvents(context.Background(), req)
	require.NoError(t, err)

	resp, err := stream.Recv()
	require.NoError(t, err)

	assert.Equal(t, accountpb.Events_OK, resp.Result)
	assert.Equal(t, 1, len(resp.Events))
	assert.Equal(t, kp1.Address(), resp.Events[0].GetAccountUpdateEvent().GetAccountInfo().GetAccountId().Value)
	assert.Equal(t, int64(10*100000), resp.Events[0].GetAccountUpdateEvent().GetAccountInfo().GetBalance())
	assert.Equal(t, int64(1), resp.Events[0].GetAccountUpdateEvent().GetAccountInfo().GetSequenceNumber())

	env.accountNotifier.OnTransaction(e, m)

	resp, err = stream.Recv()
	require.NoError(t, err)

	assert.Equal(t, accountpb.Events_OK, resp.Result)
	assert.Equal(t, 2, len(resp.Events))

	expectedBytes, err := e.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, expectedBytes, resp.Events[0].GetTransactionEvent().EnvelopeXdr)

	assert.Equal(t, kp1.Address(), resp.Events[1].GetAccountUpdateEvent().GetAccountInfo().GetAccountId().Value)
	assert.Equal(t, int64(900000), resp.Events[1].GetAccountUpdateEvent().GetAccountInfo().GetBalance())
	assert.Equal(t, int64(2), resp.Events[1].GetAccountUpdateEvent().GetAccountInfo().GetSequenceNumber())
}

func TestGetEvents_TerminateStream(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	kp1, acc1 := test.GenerateAccountID(t)
	_, acc2 := test.GenerateAccountID(t)

	e := test.GenerateTransactionEnvelope(acc1, []xdr.Operation{test.GenerateMergeOperation(nil, acc2)})
	m := test.GenerateTransactionMeta(0, []xdr.OperationMeta{
		{
			Changes: []xdr.LedgerEntryChange{
				test.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryRemoved, acc1, 2, 900000),
				test.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc2, 2, 1100000),
			},
		},
	})

	env.horizonClient.On("LoadAccount", kp1.Address()).Return(*test.GenerateHorizonAccount(kp1.Address(), "10", "1"), nil).Once()

	req := &accountpb.GetEventsRequest{AccountId: &commonpb.StellarAccountId{Value: kp1.Address()}}
	stream, err := env.client.GetEvents(context.Background(), req)
	require.NoError(t, err)

	resp, err := stream.Recv()
	require.NoError(t, err)

	assert.Equal(t, accountpb.Events_OK, resp.Result)
	assert.Equal(t, 1, len(resp.Events))
	assert.Equal(t, kp1.Address(), resp.Events[0].GetAccountUpdateEvent().GetAccountInfo().GetAccountId().Value)
	assert.Equal(t, int64(10*100000), resp.Events[0].GetAccountUpdateEvent().GetAccountInfo().GetBalance())
	assert.Equal(t, int64(1), resp.Events[0].GetAccountUpdateEvent().GetAccountInfo().GetSequenceNumber())

	env.accountNotifier.OnTransaction(e, m)

	resp, err = stream.Recv()
	require.NoError(t, err)

	assert.Equal(t, accountpb.Events_OK, resp.Result)
	assert.Equal(t, 1, len(resp.Events))

	expectedBytes, err := e.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, expectedBytes, resp.Events[0].GetTransactionEvent().EnvelopeXdr)

	resp, err = stream.Recv()
	require.Nil(t, resp)
	assert.Equal(t, err, io.EOF)
}

func TestGetEvents_NotFound(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	kp, err := keypair.Random()
	require.NoError(t, err)

	horizonErr := &horizon.Error{Problem: horizon.Problem{Status: 404}}
	env.horizonClient.On("LoadAccount", kp.Address()).Return(hProtocol.Account{}, horizonErr).Once()

	req := &accountpb.GetEventsRequest{AccountId: &commonpb.StellarAccountId{Value: kp.Address()}}
	stream, err := env.client.GetEvents(context.Background(), req)
	require.NoError(t, err)

	resp, err := stream.Recv()
	require.NoError(t, err)

	assert.Equal(t, accountpb.Events_NOT_FOUND, resp.Result)
}
