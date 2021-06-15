package server

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/kinecosystem/agora-common/headers"
	"github.com/kinecosystem/agora-common/kin/version"
	agoratestutil "github.com/kinecosystem/agora-common/testutil"
	"github.com/kinecosystem/go/clients/horizon"
	"github.com/kinecosystem/go/keypair"
	hProtocol "github.com/kinecosystem/go/protocols/horizon"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	xrate "golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v3"
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"

	"github.com/kinecosystem/agora/pkg/account"
	"github.com/kinecosystem/agora/pkg/channel"
	channelpool "github.com/kinecosystem/agora/pkg/channel/memory"
	"github.com/kinecosystem/agora/pkg/rate"
	"github.com/kinecosystem/agora/pkg/testutil"
	"github.com/kinecosystem/agora/pkg/transaction/stellar"
)

const (
	channelSalt = "somesalt"
)

type testEnv struct {
	client          accountpb.AccountClient
	hClient         *horizon.MockClient
	rootKP          *keypair.Full
	accountNotifier *AccountNotifier
	channels        []*keypair.Full

	kin2HClient         *horizon.MockClient
	kin2RootKP          *keypair.Full
	kin2AccountNotifier *AccountNotifier
	kin2Channels        []*keypair.Full
}

func setup(t *testing.T, createAccGlobalRL xrate.Limit, maxChannels int) (env testEnv, cleanup func()) {
	err := os.Setenv("AGORA_ENVIRONMENT", "test")
	require.NoError(t, err)

	conn, serv, err := agoratestutil.NewServer(
		agoratestutil.WithUnaryServerInterceptor(headers.UnaryServerInterceptor()),
		agoratestutil.WithStreamServerInterceptor(headers.StreamServerInterceptor()),
	)
	require.NoError(t, err)

	env.client = accountpb.NewAccountClient(conn)
	env.hClient = &horizon.MockClient{}
	env.kin2HClient = &horizon.MockClient{}

	env.rootKP, err = keypair.Random()
	require.NoError(t, err)

	env.kin2RootKP, err = keypair.Random()
	require.NoError(t, err)

	var channelPool channel.Pool
	var kin2ChannelPool channel.Pool
	if maxChannels > 0 {
		env.channels = make([]*keypair.Full, maxChannels)
		for i := 0; i < maxChannels; i++ {
			kp, err := channel.GenerateChannelKeypair(env.rootKP, i, channelSalt)
			require.NoError(t, err)
			env.channels[i] = kp
		}

		channelPool, err = channelpool.New(maxChannels, version.KinVersion3, env.rootKP, channelSalt)
		require.NoError(t, err)

		env.kin2Channels = make([]*keypair.Full, maxChannels)
		for i := 0; i < maxChannels; i++ {
			kp, err := channel.GenerateChannelKeypair(env.kin2RootKP, i, channelSalt)
			require.NoError(t, err)
			env.kin2Channels[i] = kp
		}

		kin2ChannelPool, err = channelpool.New(maxChannels, version.KinVersion2, env.kin2RootKP, channelSalt)
		require.NoError(t, err)
	}

	env.accountNotifier = NewAccountNotifier()
	env.kin2AccountNotifier = NewAccountNotifier()

	s, err := New(
		env.rootKP,
		env.hClient,
		env.accountNotifier,
		channelPool,
		env.kin2RootKP,
		env.kin2HClient,
		env.kin2AccountNotifier,
		kin2ChannelPool,
		account.NewLimiter(rate.NewLocalRateLimiter(createAccGlobalRL)),
	)
	require.NoError(t, err)

	serv.RegisterService(func(server *grpc.Server) {
		accountpb.RegisterAccountServer(server, s)
	})

	cleanup, err = serv.Serve()
	require.NoError(t, err)

	return env, cleanup
}

func TestCreateAccount_NoChannels(t *testing.T) {
	env, cleanup := setup(t, 5, 0)
	defer cleanup()

	kp, err := keypair.Random()
	require.NoError(t, err)

	// There are no channels, so the root account should get used
	env.hClient.On("LoadAccount", env.rootKP.Address()).Return(*testutil.GenerateHorizonAccount(env.rootKP.Address(), "100", "1"), nil).Once()

	// The account initially does not exist, then after a transaction is submitted it should.
	horizonErr := &horizon.Error{Problem: horizon.Problem{Status: 404}}
	env.hClient.On("LoadAccount", kp.Address()).Return(hProtocol.Account{}, horizonErr).Once()
	env.hClient.On("SubmitTransaction", mock.AnythingOfType("string")).Return(hProtocol.TransactionSuccess{}, nil).Once()
	env.hClient.On("LoadAccount", kp.Address()).Return(*testutil.GenerateHorizonAccount(kp.Address(), "100", "1"), nil).Once()

	req := accountpb.CreateAccountRequest{
		AccountId: &commonpb.StellarAccountId{Value: kp.Address()},
	}
	resp, err := env.client.CreateAccount(context.Background(), &req)
	require.NoError(t, err)

	assert.Equal(t, accountpb.CreateAccountResponse_OK, resp.GetResult())
	assert.Equal(t, kp.Address(), resp.GetAccountInfo().GetAccountId().Value)
	assert.Equal(t, int64(100*1e5), resp.GetAccountInfo().GetBalance())
	assert.Equal(t, int64(1), resp.GetAccountInfo().GetSequenceNumber())

	env.hClient.AssertExpectations(t)
}

func TestCreateAccount_WithChannel(t *testing.T) {
	env, cleanup := setup(t, 5, 1)
	defer cleanup()

	kp, err := keypair.Random()
	require.NoError(t, err)

	// Channels are enabled with a max of 1, so we expect the channel keypair to get used
	channelKP := env.channels[0]
	env.hClient.On("LoadAccount", channelKP.Address()).Return(*testutil.GenerateHorizonAccount(channelKP.Address(), "100", "1"), nil).Once()

	// The account initially does not exist, then after a transaction is submitted it should.
	horizonErr := &horizon.Error{Problem: horizon.Problem{Status: 404}}
	env.hClient.On("LoadAccount", kp.Address()).Return(hProtocol.Account{}, horizonErr).Once()
	env.hClient.On("SubmitTransaction", mock.AnythingOfType("string")).Return(hProtocol.TransactionSuccess{}, nil).Once()
	env.hClient.On("LoadAccount", kp.Address()).Return(*testutil.GenerateHorizonAccount(kp.Address(), "100", "1"), nil).Once()

	req := accountpb.CreateAccountRequest{
		AccountId: &commonpb.StellarAccountId{Value: kp.Address()},
	}
	resp, err := env.client.CreateAccount(context.Background(), &req)
	require.NoError(t, err)

	assert.Equal(t, accountpb.CreateAccountResponse_OK, resp.GetResult())
	assert.Equal(t, kp.Address(), resp.GetAccountInfo().GetAccountId().Value)
	assert.Equal(t, int64(100*1e5), resp.GetAccountInfo().GetBalance())
	assert.Equal(t, int64(1), resp.GetAccountInfo().GetSequenceNumber())

	env.hClient.AssertExpectations(t)
}

func TestCreateAccount_NewChannel(t *testing.T) {
	env, cleanup := setup(t, 5, 1)
	defer cleanup()

	// Channels are enabled with a max of 1, so we expect the channel keypair to get used
	channelKP := env.channels[0]
	horizonErr := &horizon.Error{Problem: horizon.Problem{Status: 404}}
	env.hClient.On("LoadAccount", channelKP.Address()).Return(hProtocol.Account{}, horizonErr).Once()

	// The channel doesn't exist initially, so the root account will get loaded to create it
	env.hClient.On("LoadAccount", env.rootKP.Address()).Return(*testutil.GenerateHorizonAccount(env.rootKP.Address(), "100", "1"), nil).Once()

	// After the transaction is submitted, the channel should exist.
	env.hClient.On("SubmitTransaction", mock.AnythingOfType("string")).Return(hProtocol.TransactionSuccess{}, nil).Once()
	env.hClient.On("LoadAccount", channelKP.Address()).Return(*testutil.GenerateHorizonAccount(channelKP.Address(), "100", "1"), nil).Once()

	kp, err := keypair.Random()
	require.NoError(t, err)

	// The account initially does not exist, then after a transaction is submitted it should.
	env.hClient.On("LoadAccount", kp.Address()).Return(hProtocol.Account{}, horizonErr).Once()
	env.hClient.On("SubmitTransaction", mock.AnythingOfType("string")).Return(hProtocol.TransactionSuccess{}, nil).Once()
	env.hClient.On("LoadAccount", kp.Address()).Return(*testutil.GenerateHorizonAccount(kp.Address(), "100", "1"), nil).Once()

	req := accountpb.CreateAccountRequest{
		AccountId: &commonpb.StellarAccountId{Value: kp.Address()},
	}
	resp, err := env.client.CreateAccount(context.Background(), &req)
	require.NoError(t, err)

	assert.Equal(t, accountpb.CreateAccountResponse_OK, resp.GetResult())
	assert.Equal(t, kp.Address(), resp.GetAccountInfo().GetAccountId().Value)
	assert.Equal(t, int64(100*1e5), resp.GetAccountInfo().GetBalance())
	assert.Equal(t, int64(1), resp.GetAccountInfo().GetSequenceNumber())

	env.hClient.AssertExpectations(t)
}

func TestCreateAccount_Kin2(t *testing.T) {
	env, cleanup := setup(t, 5, 1)
	defer cleanup()

	kp, err := keypair.Random()
	require.NoError(t, err)

	// Channels are enabled with a max of 1, so we expect the channel keypair to get used
	channelKP := env.kin2Channels[0]
	env.kin2HClient.On("LoadAccount", channelKP.Address()).Return(*testutil.GenerateHorizonAccount(channelKP.Address(), "400", "1"), nil).Once()

	// The account initially does not exist, then after a transaction is submitted it should.
	horizonErr := &horizon.Error{Problem: horizon.Problem{Status: 404}}
	env.kin2HClient.On("LoadAccount", kp.Address()).Return(hProtocol.Account{}, horizonErr).Once()
	env.kin2HClient.On("SubmitTransaction", mock.AnythingOfType("string")).Return(hProtocol.TransactionSuccess{}, nil).Once()
	env.kin2HClient.On("LoadAccount", kp.Address()).Return(*testutil.GenerateHorizonAccount(kp.Address(), "100", "1"), nil).Once()

	req := accountpb.CreateAccountRequest{
		AccountId: &commonpb.StellarAccountId{Value: kp.Address()},
	}
	resp, err := env.client.CreateAccount(testutil.GetKin2Context(context.Background()), &req)
	require.NoError(t, err)

	assert.Equal(t, accountpb.CreateAccountResponse_OK, resp.GetResult())
	assert.Equal(t, kp.Address(), resp.GetAccountInfo().GetAccountId().Value)
	assert.Equal(t, int64(100*1e5), resp.GetAccountInfo().GetBalance())
	assert.Equal(t, int64(1), resp.GetAccountInfo().GetSequenceNumber())

	env.kin2HClient.AssertExpectations(t)
}

func TestCreateAccount_Kin2ChannelUnderfunded(t *testing.T) {
	env, cleanup := setup(t, 5, 1)
	defer cleanup()

	kp, err := keypair.Random()
	require.NoError(t, err)

	// Channels are enabled with a max of 1, so we expect the channel keypair to get used
	channelKP := env.kin2Channels[0]
	env.kin2HClient.On("LoadAccount", channelKP.Address()).Return(*testutil.GenerateHorizonAccount(channelKP.Address(), "300", "1"), nil).Once()

	// The channel needs funding, so the root account gets loaded and a transaction gets submitted
	env.kin2HClient.On("LoadAccount", env.kin2RootKP.Address()).Return(*testutil.GenerateHorizonAccount(env.kin2RootKP.Address(), "1000", "1"), nil).Once()
	env.kin2HClient.On("SubmitTransaction", mock.AnythingOfType("string")).Return(hProtocol.TransactionSuccess{}, nil).Once()

	// The account initially does not exist, then after a transaction is submitted it should.
	horizonErr := &horizon.Error{Problem: horizon.Problem{Status: 404}}
	env.kin2HClient.On("LoadAccount", kp.Address()).Return(hProtocol.Account{}, horizonErr).Once()
	env.kin2HClient.On("SubmitTransaction", mock.AnythingOfType("string")).Return(hProtocol.TransactionSuccess{}, nil).Once()
	env.kin2HClient.On("LoadAccount", kp.Address()).Return(*testutil.GenerateHorizonAccount(kp.Address(), "100", "1"), nil).Once()

	req := accountpb.CreateAccountRequest{
		AccountId: &commonpb.StellarAccountId{Value: kp.Address()},
	}
	resp, err := env.client.CreateAccount(testutil.GetKin2Context(context.Background()), &req)
	require.NoError(t, err)

	assert.Equal(t, accountpb.CreateAccountResponse_OK, resp.GetResult())
	assert.Equal(t, kp.Address(), resp.GetAccountInfo().GetAccountId().Value)
	assert.Equal(t, int64(100*1e5), resp.GetAccountInfo().GetBalance())
	assert.Equal(t, int64(1), resp.GetAccountInfo().GetSequenceNumber())

	env.kin2HClient.AssertExpectations(t)
}

func TestCreateAccount_Exists(t *testing.T) {
	env, cleanup := setup(t, 5, 0)
	defer cleanup()

	kp, err := keypair.Random()
	require.NoError(t, err)

	env.hClient.On("LoadAccount", kp.Address()).Return(*testutil.GenerateHorizonAccount(kp.Address(), "100", "1"), nil).Once()

	req := accountpb.CreateAccountRequest{AccountId: &commonpb.StellarAccountId{Value: kp.Address()}}

	resp, err := env.client.CreateAccount(context.Background(), &req)
	require.NoError(t, err)

	assert.Equal(t, accountpb.CreateAccountResponse_EXISTS, resp.GetResult())
	assert.Equal(t, kp.Address(), resp.GetAccountInfo().GetAccountId().Value)
	assert.Equal(t, int64(100*1e5), resp.GetAccountInfo().GetBalance())
	assert.Equal(t, int64(1), resp.GetAccountInfo().GetSequenceNumber())

	env.hClient.AssertExpectations(t)
}

func TestCreateAccount_RateLimited(t *testing.T) {
	env, cleanup := setup(t, 5, 0)
	defer cleanup()

	kp, err := keypair.Random()
	require.NoError(t, err)

	req := accountpb.CreateAccountRequest{AccountId: &commonpb.StellarAccountId{Value: kp.Address()}}
	for i := 0; i < 5; i++ {
		env.hClient.On("LoadAccount", kp.Address()).Return(*testutil.GenerateHorizonAccount(kp.Address(), "100", "1"), nil).Once()
		_, err = env.client.CreateAccount(context.Background(), &req)
		require.NoError(t, err)
	}

	_, err = env.client.CreateAccount(context.Background(), &req)
	require.Equal(t, codes.Unavailable, status.Code(err))

	// wait until the rate limit resets
	time.Sleep(1 * time.Second)

	for i := 0; i < 5; i++ {
		env.hClient.On("LoadAccount", kp.Address()).Return(*testutil.GenerateHorizonAccount(kp.Address(), "100", "1"), nil).Once()
		_, err = env.client.CreateAccount(context.Background(), &req)
		require.NoError(t, err)
	}

	_, err = env.client.CreateAccount(context.Background(), &req)
	require.Equal(t, codes.Unavailable, status.Code(err))

	env.hClient.AssertExpectations(t)
}

func TestCreateAccount_NoRateLimit(t *testing.T) {
	env, cleanup := setup(t, xrate.Inf, 0)
	defer cleanup()

	kp, err := keypair.Random()
	require.NoError(t, err)

	req := accountpb.CreateAccountRequest{AccountId: &commonpb.StellarAccountId{Value: kp.Address()}}
	for i := 0; i < 10; i++ {
		env.hClient.On("LoadAccount", kp.Address()).Return(*testutil.GenerateHorizonAccount(kp.Address(), "100", "1"), nil).Once()
		_, err = env.client.CreateAccount(context.Background(), &req)
		require.NoError(t, err)
	}

	env.hClient.AssertExpectations(t)
}

func TestGetAccountInfo(t *testing.T) {
	env, cleanup := setup(t, -1, 0)
	defer cleanup()

	kp, err := keypair.Random()
	require.NoError(t, err)

	env.hClient.On("LoadAccount", kp.Address()).Return(*testutil.GenerateHorizonAccount(kp.Address(), "100", "1"), nil).Once()

	req := accountpb.GetAccountInfoRequest{AccountId: &commonpb.StellarAccountId{Value: kp.Address()}}
	resp, err := env.client.GetAccountInfo(context.Background(), &req)
	require.NoError(t, err)

	assert.Equal(t, accountpb.GetAccountInfoResponse_OK, resp.Result)
	assert.Equal(t, kp.Address(), resp.GetAccountInfo().GetAccountId().Value)
	assert.Equal(t, int64(100*1e5), resp.GetAccountInfo().GetBalance())
	assert.Equal(t, int64(1), resp.GetAccountInfo().GetSequenceNumber())

	env.hClient.AssertExpectations(t)
}

func TestGetAccountInfoNotFound(t *testing.T) {
	env, cleanup := setup(t, -1, 0)
	defer cleanup()

	kp, err := keypair.Random()
	require.NoError(t, err)

	horizonErr := &horizon.Error{Problem: horizon.Problem{Status: 404}}
	env.hClient.On("LoadAccount", kp.Address()).Return(hProtocol.Account{}, horizonErr).Once()

	req := accountpb.GetAccountInfoRequest{AccountId: &commonpb.StellarAccountId{Value: kp.Address()}}
	resp, err := env.client.GetAccountInfo(context.Background(), &req)
	require.NoError(t, err)

	assert.Equal(t, accountpb.GetAccountInfoResponse_NOT_FOUND, resp.Result)

	env.hClient.AssertExpectations(t)
}

func TestGetAccountInfo_Kin2(t *testing.T) {
	env, cleanup := setup(t, -1, 0)
	defer cleanup()

	kp, err := keypair.Random()
	require.NoError(t, err)

	env.kin2HClient.On("LoadAccount", kp.Address()).Return(*testutil.GenerateKin2HorizonAccount(kp.Address(), "100", "1"), nil).Once()

	req := accountpb.GetAccountInfoRequest{AccountId: &commonpb.StellarAccountId{Value: kp.Address()}}
	resp, err := env.client.GetAccountInfo(testutil.GetKin2Context(context.Background()), &req)
	require.NoError(t, err)

	assert.Equal(t, accountpb.GetAccountInfoResponse_OK, resp.Result)
	assert.Equal(t, kp.Address(), resp.GetAccountInfo().GetAccountId().Value)
	assert.Equal(t, int64(100*1e5), resp.GetAccountInfo().GetBalance())
	assert.Equal(t, int64(1), resp.GetAccountInfo().GetSequenceNumber())

	env.hClient.AssertExpectations(t)
}

func TestGetEvents_HappyPath(t *testing.T) {
	env, cleanup := setup(t, -1, 0)
	defer cleanup()

	kp1, acc1 := testutil.GenerateAccountID(t)
	_, acc2 := testutil.GenerateAccountID(t)

	e := testutil.GenerateTransactionEnvelope(acc1, 1, []xdr.Operation{testutil.GeneratePaymentOperation(nil, acc2)})
	r := testutil.GenerateTransactionResult(xdr.TransactionResultCodeTxSuccess, make([]xdr.OperationResult, 0))
	m := testutil.GenerateTransactionMeta(0, []xdr.OperationMeta{
		{
			Changes: []xdr.LedgerEntryChange{
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc1, 2, 900000),
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc2, 2, 1100000),
			},
		},
	})

	env.hClient.On("LoadAccount", kp1.Address()).Return(*testutil.GenerateHorizonAccount(kp1.Address(), "10", "1"), nil).Once()

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

	env.accountNotifier.OnTransaction(stellar.XDRData{Envelope: e, Result: r, Meta: m})

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
	env, cleanup := setup(t, -1, 0)
	defer cleanup()

	kp1, acc1 := testutil.GenerateAccountID(t)
	_, acc2 := testutil.GenerateAccountID(t)

	e := testutil.GenerateTransactionEnvelope(acc1, 1, []xdr.Operation{testutil.GeneratePaymentOperation(nil, acc2)})
	r := testutil.GenerateTransactionResult(xdr.TransactionResultCodeTxSuccess, make([]xdr.OperationResult, 0))
	m := testutil.GenerateTransactionMeta(0, []xdr.OperationMeta{
		{
			Changes: []xdr.LedgerEntryChange{
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc1, 2, 900000),
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc2, 2, 1100000),
			},
		},
	})

	env.hClient.On("LoadAccount", kp1.Address()).Return(*testutil.GenerateHorizonAccount(kp1.Address(), "10", "1"), nil).Once()

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
		env.accountNotifier.OnTransaction(stellar.XDRData{Envelope: e, Result: r, Meta: m})
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

	env.accountNotifier.OnTransaction(stellar.XDRData{Envelope: e, Result: r, Meta: m})

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
	env, cleanup := setup(t, -1, 0)
	defer cleanup()

	kp1, acc1 := testutil.GenerateAccountID(t)
	_, acc2 := testutil.GenerateAccountID(t)

	e := testutil.GenerateTransactionEnvelope(acc1, 1, []xdr.Operation{testutil.GeneratePaymentOperation(nil, acc2)})
	r := testutil.GenerateTransactionResult(xdr.TransactionResultCodeTxSuccess, make([]xdr.OperationResult, 0))
	m := testutil.GenerateTransactionMeta(0, make([]xdr.OperationMeta, 0))

	env.hClient.On("LoadAccount", kp1.Address()).Return(*testutil.GenerateHorizonAccount(kp1.Address(), "10", "1"), nil).Once()

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
	env.hClient.On("LoadAccount", kp1.Address()).Return(*testutil.GenerateHorizonAccount(kp1.Address(), "9", "2"), nil).Once()

	env.accountNotifier.OnTransaction(stellar.XDRData{Envelope: e, Result: r, Meta: m})

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
	env, cleanup := setup(t, -1, 0)
	defer cleanup()

	kp1, acc1 := testutil.GenerateAccountID(t)
	_, acc2 := testutil.GenerateAccountID(t)

	e := testutil.GenerateTransactionEnvelope(acc1, 1, []xdr.Operation{testutil.GeneratePaymentOperation(nil, acc2)})
	r := testutil.GenerateTransactionResult(xdr.TransactionResultCodeTxSuccess, make([]xdr.OperationResult, 0))
	m := testutil.GenerateTransactionMeta(0, make([]xdr.OperationMeta, 0))

	env.hClient.On("LoadAccount", kp1.Address()).Return(*testutil.GenerateHorizonAccount(kp1.Address(), "10", "1"), nil).Once()

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
	env.hClient.On("LoadAccount", kp1.Address()).Return(hProtocol.Account{}, horizonErr).Once()

	env.accountNotifier.OnTransaction(stellar.XDRData{Envelope: e, Result: r, Meta: m})

	resp, err = stream.Recv()
	require.NoError(t, err)

	assert.Equal(t, accountpb.Events_OK, resp.Result)
	assert.Equal(t, 1, len(resp.Events))

	expectedBytes, err := e.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, expectedBytes, resp.Events[0].GetTransactionEvent().EnvelopeXdr)
}

func TestGetEvents_AccountRemoved(t *testing.T) {
	env, cleanup := setup(t, -1, 0)
	defer cleanup()

	kp1, acc1 := testutil.GenerateAccountID(t)
	_, acc2 := testutil.GenerateAccountID(t)

	e := testutil.GenerateTransactionEnvelope(acc1, 1, []xdr.Operation{testutil.GenerateMergeOperation(nil, acc2)})
	r := testutil.GenerateTransactionResult(xdr.TransactionResultCodeTxSuccess, make([]xdr.OperationResult, 0))
	m := testutil.GenerateTransactionMeta(0, []xdr.OperationMeta{
		{
			Changes: []xdr.LedgerEntryChange{
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryRemoved, acc1, 2, 900000),
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc2, 2, 1100000),
			},
		},
	})

	env.hClient.On("LoadAccount", kp1.Address()).Return(*testutil.GenerateHorizonAccount(kp1.Address(), "10", "1"), nil).Once()

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

	env.accountNotifier.OnTransaction(stellar.XDRData{Envelope: e, Result: r, Meta: m})

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
	env, cleanup := setup(t, -1, 0)
	defer cleanup()

	kp, err := keypair.Random()
	require.NoError(t, err)

	horizonErr := &horizon.Error{Problem: horizon.Problem{Status: 404}}
	env.hClient.On("LoadAccount", kp.Address()).Return(hProtocol.Account{}, horizonErr).Once()

	req := &accountpb.GetEventsRequest{AccountId: &commonpb.StellarAccountId{Value: kp.Address()}}
	stream, err := env.client.GetEvents(context.Background(), req)
	require.NoError(t, err)

	resp, err := stream.Recv()
	require.NoError(t, err)

	assert.Equal(t, accountpb.Events_NOT_FOUND, resp.Result)
}

func TestGetEvents_Kin2(t *testing.T) {
	env, cleanup := setup(t, -1, 0)
	defer cleanup()

	kp1, acc1 := testutil.GenerateAccountID(t)
	_, acc2 := testutil.GenerateAccountID(t)

	e := testutil.GenerateTransactionEnvelope(acc1, 1, []xdr.Operation{testutil.GeneratePaymentOperation(nil, acc2)})
	r := testutil.GenerateTransactionResult(xdr.TransactionResultCodeTxSuccess, make([]xdr.OperationResult, 0))

	// Kin 2 transaction metas contain trust line entry updates instead of account entry updates, so we have to rely on
	// loading the account
	lecAcc1 := xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
		Updated: &xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeTrustline,
				TrustLine: &xdr.TrustLineEntry{
					AccountId: acc1,
					Balance:   90000000,
				},
			},
		},
	}
	lecAcc2 := xdr.LedgerEntryChange{
		Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
		Updated: &xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeTrustline,
				TrustLine: &xdr.TrustLineEntry{
					AccountId: acc2,
					Balance:   110000000,
				},
			},
		},
	}

	m := testutil.GenerateTransactionMeta(0, []xdr.OperationMeta{
		{
			Changes: []xdr.LedgerEntryChange{
				lecAcc1,
				lecAcc2,
			},
		},
	})

	env.kin2HClient.On("LoadAccount", kp1.Address()).Return(*testutil.GenerateKin2HorizonAccount(kp1.Address(), "10", "1"), nil).Once()

	req := &accountpb.GetEventsRequest{AccountId: &commonpb.StellarAccountId{Value: kp1.Address()}}
	stream, err := env.client.GetEvents(testutil.GetKin2Context(context.Background()), req)
	require.NoError(t, err)

	resp, err := stream.Recv()
	require.NoError(t, err)

	assert.Equal(t, accountpb.Events_OK, resp.Result)
	assert.Equal(t, 1, len(resp.Events))
	assert.Equal(t, kp1.Address(), resp.Events[0].GetAccountUpdateEvent().GetAccountInfo().GetAccountId().Value)
	assert.Equal(t, int64(10*1e5), resp.Events[0].GetAccountUpdateEvent().GetAccountInfo().GetBalance())
	assert.Equal(t, int64(1), resp.Events[0].GetAccountUpdateEvent().GetAccountInfo().GetSequenceNumber())

	// Successfully obtain account info; both the transaction and account events should get sent
	env.kin2HClient.On("LoadAccount", kp1.Address()).Return(*testutil.GenerateKin2HorizonAccount(kp1.Address(), "9", "2"), nil).Once()

	env.kin2AccountNotifier.OnTransaction(stellar.XDRData{Envelope: e, Result: r, Meta: m})

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
