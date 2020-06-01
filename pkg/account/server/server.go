package server

import (
	"context"
	"strconv"

	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/go/build"
	"github.com/kinecosystem/go/clients/horizon"
	"github.com/kinecosystem/go/keypair"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v3"
)

const (
	eventStreamBufferSize = 64
)

type server struct {
	log             *logrus.Entry
	rootAccountKP   *keypair.Full
	horizonClient   horizon.ClientInterface
	accountNotifier *AccountNotifier
}

// New returns a new account server
func New(rootAccountKP *keypair.Full, horizonClient horizon.ClientInterface, accountNotifier *AccountNotifier) accountpb.AccountServer {
	return &server{
		log:             logrus.StandardLogger().WithField("type", "account/server"),
		rootAccountKP:   rootAccountKP,
		horizonClient:   horizonClient,
		accountNotifier: accountNotifier,
	}
}

// CreateAccount implements AccountServer.CreateAccount
func (s *server) CreateAccount(ctx context.Context, req *accountpb.CreateAccountRequest) (*accountpb.CreateAccountResponse, error) {
	log := s.log.WithField("method", "CreateAccount")

	// Check if account exists on the blockchain
	horizonAccount, err := s.horizonClient.LoadAccount(req.AccountId.Value)
	if err == nil {
		accountInfo, err := parseAccountInfo(horizonAccount)
		if err != nil {
			log.WithError(err).Warn("Failed to parse account info from horizon account")
			return nil, status.Error(codes.Internal, err.Error())
		}

		return &accountpb.CreateAccountResponse{
			Result:      accountpb.CreateAccountResponse_EXISTS,
			AccountInfo: accountInfo,
		}, nil
	}

	horizonError, ok := err.(*horizon.Error)
	// 404 indicates that the account doesn't exist, which is acceptable
	if !ok || (ok && horizonError.Problem.Status != 404) {
		log.WithError(err).Warn("Failed to check if account exists")
		return nil, status.Error(codes.Internal, err.Error())
	}

	rootHorizonAccount, err := s.horizonClient.LoadAccount(s.rootAccountKP.Address())
	if err != nil {
		log.WithError(err).Warn("Failed to load root account")
		return nil, status.Error(codes.Internal, err.Error())
	}

	// sequence number is typically represented with an int64, but the Horizon client sequence number mutator uses a
	// uint64, so we parse it accordingly here.
	prevSeq, err := strconv.ParseUint(rootHorizonAccount.Sequence, 10, 64)
	if err != nil {
		log.WithError(err).Warn("Failed to parse root account sequence number")
		return nil, status.Error(codes.Internal, err.Error())
	}

	network, err := kin.GetNetwork()
	if err != nil {
		log.WithError(err).Warn("Failed to get network")
		return nil, status.Error(codes.Internal, err.Error())
	}

	encodedTx, err := buildSignEncodeTransaction(s.rootAccountKP,
		build.SourceAccount{AddressOrSeed: s.rootAccountKP.Address()},
		build.Sequence{Sequence: prevSeq + 1},
		network,
		build.CreateAccount(
			build.Destination{AddressOrSeed: req.AccountId.Value},
			build.NativeAmount{Amount: "0"},
		))

	if err != nil {
		log.WithError(err).Warn("Failed to create transaction")
		return nil, status.Error(codes.Internal, err.Error())
	}

	_, err = s.horizonClient.SubmitTransaction(encodedTx)
	if err != nil {
		log.WithError(err).Warn("Failed to submit transaction")
		return nil, status.Error(codes.Internal, err.Error())
	}

	horizonAccount, err = s.horizonClient.LoadAccount(req.AccountId.Value)
	if err != nil {
		log.WithError(err).Warn("Failed to load account from horizon after creation")
		return nil, status.Error(codes.Internal, err.Error())
	}

	accountInfo, err := parseAccountInfo(horizonAccount)
	if err != nil {
		log.WithError(err).Warn("Failed to parse account info from horizon account")
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &accountpb.CreateAccountResponse{
		Result:      accountpb.CreateAccountResponse_OK,
		AccountInfo: accountInfo,
	}, nil
}

// GetAccountInfo implements AccountServer.GetAccountInfo
func (s *server) GetAccountInfo(ctx context.Context, req *accountpb.GetAccountInfoRequest) (*accountpb.GetAccountInfoResponse, error) {
	log := s.log.WithField("method", "GetAccountInfo")

	horizonAccount, err := s.horizonClient.LoadAccount(req.AccountId.Value)
	if err != nil {
		horizonError, ok := err.(*horizon.Error)
		if ok && horizonError.Problem.Status == 404 {
			return &accountpb.GetAccountInfoResponse{Result: accountpb.GetAccountInfoResponse_NOT_FOUND}, nil
		}

		log.WithError(err).Warn("Failed to load account")
		return nil, status.Error(codes.Internal, err.Error())
	}

	accountInfo, err := parseAccountInfo(horizonAccount)
	if err != nil {
		log.WithError(err).Warn("Failed to parse account info from horizon account")
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &accountpb.GetAccountInfoResponse{
		Result:      accountpb.GetAccountInfoResponse_OK,
		AccountInfo: accountInfo,
	}, nil
}

func (s *server) GetEvents(req *accountpb.GetEventsRequest, stream accountpb.Account_GetEventsServer) error {
	log := s.log.WithField("method", "GetEvents")

	horizonAccount, err := s.horizonClient.LoadAccount(req.AccountId.Value)
	if err != nil {
		horizonError, ok := err.(*horizon.Error)
		if ok && horizonError.Problem.Status == 404 {
			sendErr := stream.Send(&accountpb.Events{Result: accountpb.Events_NOT_FOUND})
			if sendErr != nil {
				return status.Error(codes.Internal, err.Error())
			}
			return nil
		}

		log.WithError(err).Warn("Failed to load account")
		return status.Error(codes.Internal, err.Error())
	}

	accountInfo, err := parseAccountInfo(horizonAccount)
	if err != nil {
		log.WithError(err).Warn("Failed to parse account info from horizon account")
		return status.Error(codes.Internal, err.Error())
	}

	err = stream.Send(&accountpb.Events{
		Events: []*accountpb.Event{
			{
				Type: &accountpb.Event_AccountUpdateEvent{
					AccountUpdateEvent: &accountpb.AccountUpdateEvent{
						AccountInfo: accountInfo,
					},
				},
			},
		},
	})
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	as := newEventStream(eventStreamBufferSize)
	s.accountNotifier.AddStream(req.AccountId.Value, as)

	defer func() {
		s.accountNotifier.RemoveStream(req.AccountId.Value, as)
	}()

	for {
		select {
		case events, ok := <-as.streamCh:
			if !ok {
				return status.Error(codes.Aborted, "")
			}

			err := stream.Send(&events)
			if err != nil {
				log.WithError(err).Info("failed to send events")
				return err
			}
		case <-stream.Context().Done():
			log.Debug("Stream context cancelled, ending stream")
			return status.Error(codes.Canceled, "")
		}
	}
}

// buildSignEncodeTransaction builds a transaction with the provided transaction mutators, signs it with the provided
// keypair and returns the base 64 XDR representation of the transaction envelope ready for submission to the network.
func buildSignEncodeTransaction(keypair *keypair.Full, muts ...build.TransactionMutator) (string, error) {
	tx, err := build.Transaction(muts...)
	if err != nil {
		return "", errors.Wrap(err, "Failed to build transaction")
	}

	signedTx, err := tx.Sign(keypair.Seed())
	if err != nil {
		return "", errors.Wrap(err, "Failed to sign transaction")
	}

	encodedTx, err := signedTx.Base64()
	if err != nil {
		return "", errors.Wrap(err, "Failed to encode transaction")
	}

	return encodedTx, nil
}
