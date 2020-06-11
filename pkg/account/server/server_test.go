package server

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/go-redis/redis_rate/v8"
	redistest "github.com/kinecosystem/agora-common/redis/test"
	agoratestutil "github.com/kinecosystem/agora-common/testutil"
	"github.com/kinecosystem/go/clients/horizon"
	"github.com/kinecosystem/go/keypair"
	hProtocol "github.com/kinecosystem/go/protocols/horizon"
	"github.com/ory/dockertest"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v3"
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"

	"github.com/kinecosystem/agora/pkg/testutil"
)

var (
	redisConnString string
)

type testEnv struct {
	client          accountpb.AccountClient
	horizonClient   *horizon.MockClient
	rootAccountKP   *keypair.Full
	accountNotifier *AccountNotifier
}

func TestMain(m *testing.M) {
	log := logrus.StandardLogger()

	pool, err := dockertest.NewPool("")
	if err != nil {
		log.WithError(err).Error("Error creating docker pool")
		os.Exit(1)
	}

	var cleanUpFunc func()
	redisConnString, cleanUpFunc, err = redistest.StartRedis(context.Background(), pool)
	if err != nil {
		log.WithError(err).Error("Error starting redis connection")
		os.Exit(1)
	}

	code := m.Run()
	cleanUpFunc()
	os.Exit(code)
}

func setup(t *testing.T) (env testEnv, cleanup func()) {
	err := os.Setenv("AGORA_ENVIRONMENT", "test")
	require.NoError(t, err)

	conn, serv, err := agoratestutil.NewServer()
	require.NoError(t, err)

	env.client = accountpb.NewAccountClient(conn)
	env.horizonClient = &horizon.MockClient{}

	kp, err := keypair.Random()
	require.NoError(t, err)
	env.rootAccountKP = kp

	env.accountNotifier = NewAccountNotifier(env.horizonClient)

	ring := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{
			"server1": redisConnString,
		},
	})
	limiter := redis_rate.NewLimiter(ring)
	// reset the global rate limit (the key format is hardcoded inside redis_rate/rate.go)
	ring.Del(fmt.Sprintf("rate:%s", globalRateLimitKey))

	s := New(env.rootAccountKP, env.horizonClient, env.accountNotifier, limiter, &Config{CreateAccountGlobalLimit: 5})
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
	env.horizonClient.On("LoadAccount", kp.Address()).Return(*testutil.GenerateHorizonAccount(kp.Address(), "100", "1"), nil).Once()

	req := accountpb.CreateAccountRequest{
		AccountId: &commonpb.StellarAccountId{Value: kp.Address()},
	}
	resp, err := env.client.CreateAccount(context.Background(), &req)
	require.NoError(t, err)

	assert.Equal(t, accountpb.CreateAccountResponse_OK, resp.GetResult())
	assert.Equal(t, kp.Address(), resp.GetAccountInfo().GetAccountId().Value)
	assert.Equal(t, int64(100*1e5), resp.GetAccountInfo().GetBalance())
	assert.Equal(t, int64(1), resp.GetAccountInfo().GetSequenceNumber())

	env.horizonClient.AssertExpectations(t)
}

func TestCreateAccount_Exists(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	kp, err := keypair.Random()
	require.NoError(t, err)

	env.horizonClient.On("LoadAccount", kp.Address()).Return(*testutil.GenerateHorizonAccount(kp.Address(), "100", "1"), nil).Once()

	req := accountpb.CreateAccountRequest{AccountId: &commonpb.StellarAccountId{Value: kp.Address()}}

	resp, err := env.client.CreateAccount(context.Background(), &req)
	require.NoError(t, err)

	assert.Equal(t, accountpb.CreateAccountResponse_EXISTS, resp.GetResult())
	assert.Equal(t, kp.Address(), resp.GetAccountInfo().GetAccountId().Value)
	assert.Equal(t, int64(100*1e5), resp.GetAccountInfo().GetBalance())
	assert.Equal(t, int64(1), resp.GetAccountInfo().GetSequenceNumber())

	env.horizonClient.AssertExpectations(t)
}

func TestCreateAccount_RateLimited(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	kp, err := keypair.Random()
	require.NoError(t, err)

	req := accountpb.CreateAccountRequest{AccountId: &commonpb.StellarAccountId{Value: kp.Address()}}
	for i := 0; i < 5; i++ {
		env.horizonClient.On("LoadAccount", kp.Address()).Return(*testutil.GenerateHorizonAccount(kp.Address(), "100", "1"), nil).Once()
		_, err = env.client.CreateAccount(context.Background(), &req)
		require.NoError(t, err)
	}

	_, err = env.client.CreateAccount(context.Background(), &req)
	require.Equal(t, codes.Unavailable, status.Code(err))

	// wait until the rate limit resets
	time.Sleep(1 * time.Second)

	for i := 0; i < 5; i++ {
		env.horizonClient.On("LoadAccount", kp.Address()).Return(*testutil.GenerateHorizonAccount(kp.Address(), "100", "1"), nil).Once()
		_, err = env.client.CreateAccount(context.Background(), &req)
		require.NoError(t, err)
	}

	_, err = env.client.CreateAccount(context.Background(), &req)
	require.Equal(t, codes.Unavailable, status.Code(err))

	env.horizonClient.AssertExpectations(t)
}

func TestGetAccountInfo(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	kp, err := keypair.Random()
	require.NoError(t, err)

	env.horizonClient.On("LoadAccount", kp.Address()).Return(*testutil.GenerateHorizonAccount(kp.Address(), "100", "1"), nil).Once()

	req := accountpb.GetAccountInfoRequest{AccountId: &commonpb.StellarAccountId{Value: kp.Address()}}
	resp, err := env.client.GetAccountInfo(context.Background(), &req)
	require.NoError(t, err)

	assert.Equal(t, accountpb.GetAccountInfoResponse_OK, resp.Result)
	assert.Equal(t, kp.Address(), resp.GetAccountInfo().GetAccountId().Value)
	assert.Equal(t, int64(100*1e5), resp.GetAccountInfo().GetBalance())
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

	kp1, acc1 := testutil.GenerateAccountID(t)
	_, acc2 := testutil.GenerateAccountID(t)

	e := testutil.GenerateTransactionEnvelope(acc1, []xdr.Operation{testutil.GeneratePaymentOperation(nil, acc2)})
	m := testutil.GenerateTransactionMeta(0, []xdr.OperationMeta{
		{
			Changes: []xdr.LedgerEntryChange{
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc1, 2, 900000),
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc2, 2, 1100000),
			},
		},
	})

	env.horizonClient.On("LoadAccount", kp1.Address()).Return(*testutil.GenerateHorizonAccount(kp1.Address(), "10", "1"), nil).Once()

	req := &accountpb.GetEventsRequest{AccountId: &commonpb.StellarAccountId{Value: kp1.Address()}}
	stream, err := env.client.GetEvents(context.Background(), req)
	require.NoError(t, err)

	resp, err := stream.Recv()
	require.NoError(t, err)

	assert.Equal(t, accountpb.Events_OK, resp.Result)
	assert.Equal(t, 1, len(resp.Events))
	assert.Equal(t, kp1.Address(), resp.Events[0].GetAccountUpdateEvent().GetAccountInfo().GetAccountId().Value)
	assert.Equal(t, int64(10*1e5), resp.Events[0].GetAccountUpdateEvent().GetAccountInfo().GetBalance())
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
	assert.Equal(t, int64(9*1e5), resp.Events[1].GetAccountUpdateEvent().GetAccountInfo().GetBalance())
	assert.Equal(t, int64(2), resp.Events[1].GetAccountUpdateEvent().GetAccountInfo().GetSequenceNumber())
}

func TestGetEvents_Batched(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	kp1, acc1 := testutil.GenerateAccountID(t)
	_, acc2 := testutil.GenerateAccountID(t)

	e := testutil.GenerateTransactionEnvelope(acc1, []xdr.Operation{testutil.GeneratePaymentOperation(nil, acc2)})
	m := testutil.GenerateTransactionMeta(0, []xdr.OperationMeta{
		{
			Changes: []xdr.LedgerEntryChange{
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc1, 2, 900000),
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc2, 2, 1100000),
			},
		},
	})

	env.horizonClient.On("LoadAccount", kp1.Address()).Return(*testutil.GenerateHorizonAccount(kp1.Address(), "10", "1"), nil).Once()

	req := &accountpb.GetEventsRequest{AccountId: &commonpb.StellarAccountId{Value: kp1.Address()}}
	stream, err := env.client.GetEvents(context.Background(), req)
	require.NoError(t, err)

	resp, err := stream.Recv()
	require.NoError(t, err)

	assert.Equal(t, accountpb.Events_OK, resp.Result)
	assert.Equal(t, 1, len(resp.Events))
	assert.Equal(t, kp1.Address(), resp.Events[0].GetAccountUpdateEvent().GetAccountInfo().GetAccountId().Value)
	assert.Equal(t, int64(10*1e5), resp.Events[0].GetAccountUpdateEvent().GetAccountInfo().GetBalance())
	assert.Equal(t, int64(1), resp.Events[0].GetAccountUpdateEvent().GetAccountInfo().GetSequenceNumber())

	// Two events gets sent for each transaction
	for i := 0; i < 64; i++ {
		env.accountNotifier.OnTransaction(e, m)
	}

	resp, err = stream.Recv()
	require.NoError(t, err)

	assert.Equal(t, accountpb.Events_OK, resp.Result)
	assert.Equal(t, 128, len(resp.Events))

	for i := 0; i < 128; i += 2 {
		expectedBytes, err := e.MarshalBinary()
		require.NoError(t, err)
		require.Equal(t, expectedBytes, resp.Events[i].GetTransactionEvent().EnvelopeXdr)

		assert.Equal(t, kp1.Address(), resp.Events[i+1].GetAccountUpdateEvent().GetAccountInfo().GetAccountId().Value)
		assert.Equal(t, int64(9*1e5), resp.Events[i+1].GetAccountUpdateEvent().GetAccountInfo().GetBalance())
		assert.Equal(t, int64(2), resp.Events[i+1].GetAccountUpdateEvent().GetAccountInfo().GetSequenceNumber())
	}

	env.accountNotifier.OnTransaction(e, m)

	resp, err = stream.Recv()
	require.NoError(t, err)

	assert.Equal(t, accountpb.Events_OK, resp.Result)
	assert.Equal(t, 2, len(resp.Events))

	expectedBytes, err := e.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, expectedBytes, resp.Events[0].GetTransactionEvent().EnvelopeXdr)

	assert.Equal(t, kp1.Address(), resp.Events[1].GetAccountUpdateEvent().GetAccountInfo().GetAccountId().Value)
	assert.Equal(t, int64(9*1e5), resp.Events[1].GetAccountUpdateEvent().GetAccountInfo().GetBalance())
	assert.Equal(t, int64(2), resp.Events[1].GetAccountUpdateEvent().GetAccountInfo().GetSequenceNumber())
}

func TestGetEvents_LoadAccount(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	kp1, acc1 := testutil.GenerateAccountID(t)
	_, acc2 := testutil.GenerateAccountID(t)

	e := testutil.GenerateTransactionEnvelope(acc1, []xdr.Operation{testutil.GeneratePaymentOperation(nil, acc2)})
	m := testutil.GenerateTransactionMeta(0, make([]xdr.OperationMeta, 0))

	env.horizonClient.On("LoadAccount", kp1.Address()).Return(*testutil.GenerateHorizonAccount(kp1.Address(), "10", "1"), nil).Once()

	req := &accountpb.GetEventsRequest{AccountId: &commonpb.StellarAccountId{Value: kp1.Address()}}
	stream, err := env.client.GetEvents(context.Background(), req)
	require.NoError(t, err)

	resp, err := stream.Recv()
	require.NoError(t, err)

	assert.Equal(t, accountpb.Events_OK, resp.Result)
	assert.Equal(t, 1, len(resp.Events))
	assert.Equal(t, kp1.Address(), resp.Events[0].GetAccountUpdateEvent().GetAccountInfo().GetAccountId().Value)
	assert.Equal(t, int64(10*1e5), resp.Events[0].GetAccountUpdateEvent().GetAccountInfo().GetBalance())
	assert.Equal(t, int64(1), resp.Events[0].GetAccountUpdateEvent().GetAccountInfo().GetSequenceNumber())

	// Successfully obtain account info; both the transaction and account events should get sent
	env.horizonClient.On("LoadAccount", kp1.Address()).Return(*testutil.GenerateHorizonAccount(kp1.Address(), "9", "2"), nil).Once()

	env.accountNotifier.OnTransaction(e, m)

	resp, err = stream.Recv()
	require.NoError(t, err)

	assert.Equal(t, accountpb.Events_OK, resp.Result)
	assert.Equal(t, 2, len(resp.Events))

	expectedBytes, err := e.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, expectedBytes, resp.Events[0].GetTransactionEvent().EnvelopeXdr)

	assert.Equal(t, kp1.Address(), resp.Events[1].GetAccountUpdateEvent().GetAccountInfo().GetAccountId().Value)
	assert.Equal(t, int64(9*1e5), resp.Events[1].GetAccountUpdateEvent().GetAccountInfo().GetBalance())
	assert.Equal(t, int64(2), resp.Events[1].GetAccountUpdateEvent().GetAccountInfo().GetSequenceNumber())
}

func TestGetEvents_LoadAccountFailure(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	kp1, acc1 := testutil.GenerateAccountID(t)
	_, acc2 := testutil.GenerateAccountID(t)

	e := testutil.GenerateTransactionEnvelope(acc1, []xdr.Operation{testutil.GeneratePaymentOperation(nil, acc2)})
	m := testutil.GenerateTransactionMeta(0, make([]xdr.OperationMeta, 0))

	env.horizonClient.On("LoadAccount", kp1.Address()).Return(*testutil.GenerateHorizonAccount(kp1.Address(), "10", "1"), nil).Once()

	req := &accountpb.GetEventsRequest{AccountId: &commonpb.StellarAccountId{Value: kp1.Address()}}
	stream, err := env.client.GetEvents(context.Background(), req)
	require.NoError(t, err)

	resp, err := stream.Recv()
	require.NoError(t, err)

	assert.Equal(t, accountpb.Events_OK, resp.Result)
	assert.Equal(t, 1, len(resp.Events))
	assert.Equal(t, kp1.Address(), resp.Events[0].GetAccountUpdateEvent().GetAccountInfo().GetAccountId().Value)
	assert.Equal(t, int64(10*1e5), resp.Events[0].GetAccountUpdateEvent().GetAccountInfo().GetBalance())
	assert.Equal(t, int64(1), resp.Events[0].GetAccountUpdateEvent().GetAccountInfo().GetSequenceNumber())

	// Unable to get account info; only the transaction event should get sent
	horizonErr := &horizon.Error{Problem: horizon.Problem{Status: 500}}
	env.horizonClient.On("LoadAccount", kp1.Address()).Return(hProtocol.Account{}, horizonErr).Once()

	env.accountNotifier.OnTransaction(e, m)

	resp, err = stream.Recv()
	require.NoError(t, err)

	assert.Equal(t, accountpb.Events_OK, resp.Result)
	assert.Equal(t, 1, len(resp.Events))

	expectedBytes, err := e.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, expectedBytes, resp.Events[0].GetTransactionEvent().EnvelopeXdr)
}

func TestGetEvents_AccountRemoved(t *testing.T) {
	env, cleanup := setup(t)
	defer cleanup()

	kp1, acc1 := testutil.GenerateAccountID(t)
	_, acc2 := testutil.GenerateAccountID(t)

	e := testutil.GenerateTransactionEnvelope(acc1, []xdr.Operation{testutil.GenerateMergeOperation(nil, acc2)})
	m := testutil.GenerateTransactionMeta(0, []xdr.OperationMeta{
		{
			Changes: []xdr.LedgerEntryChange{
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryRemoved, acc1, 2, 900000),
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc2, 2, 1100000),
			},
		},
	})

	env.horizonClient.On("LoadAccount", kp1.Address()).Return(*testutil.GenerateHorizonAccount(kp1.Address(), "10", "1"), nil).Once()

	req := &accountpb.GetEventsRequest{AccountId: &commonpb.StellarAccountId{Value: kp1.Address()}}
	stream, err := env.client.GetEvents(context.Background(), req)
	require.NoError(t, err)

	resp, err := stream.Recv()
	require.NoError(t, err)

	assert.Equal(t, accountpb.Events_OK, resp.Result)
	assert.Equal(t, 1, len(resp.Events))
	assert.Equal(t, kp1.Address(), resp.Events[0].GetAccountUpdateEvent().GetAccountInfo().GetAccountId().Value)
	assert.Equal(t, int64(10*1e5), resp.Events[0].GetAccountUpdateEvent().GetAccountInfo().GetBalance())
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
