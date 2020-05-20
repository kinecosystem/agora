package server

import (
	"context"
	"strconv"

	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/go/amount"
	horizonbuild "github.com/kinecosystem/go/build"
	horizonclient "github.com/kinecosystem/go/clients/horizon"
	"github.com/kinecosystem/go/keypair"
	horizonprotocols "github.com/kinecosystem/go/protocols/horizon"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	accountpb "github.com/kinecosystem/kin-api-internal/genproto/account/v3"
	commonpb "github.com/kinecosystem/kin-api-internal/genproto/common/v3"
)

type server struct {
	log           *logrus.Entry
	rootAccountKP *keypair.Full
	horizonClient horizonclient.ClientInterface
}

// New returns a new account server
func New(rootAccountKP *keypair.Full, horizonClient horizonclient.ClientInterface) *server {
	return &server{
		log:           logrus.StandardLogger().WithField("type", "account/server"),
		rootAccountKP: rootAccountKP,
		horizonClient: horizonClient,
	}
}

// CreateAccount implements AccountServer.CreateAccount
func (s *server) CreateAccount(ctx context.Context, req *accountpb.CreateAccountRequest) (*accountpb.CreateAccountResponse, error) {
	log := s.log.WithField("method", "CreateAccount")

	// Check if account exists on the blockchain
	horizonAccount, err := s.horizonClient.LoadAccount(req.AccountId.Value)
	if err == nil {
		accountInfo, err := parseAccountInfo(req.AccountId, horizonAccount)
		if err != nil {
			log.WithError(err).Warn("Failed to parse account info from horizon account")
			return nil, status.Error(codes.Internal, err.Error())
		}

		return &accountpb.CreateAccountResponse{
			Result:      accountpb.CreateAccountResponse_EXISTS,
			AccountInfo: accountInfo,
		}, nil
	}

	horizonError, ok := err.(*horizonclient.Error)
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
		horizonbuild.SourceAccount{AddressOrSeed: s.rootAccountKP.Address()},
		horizonbuild.Sequence{Sequence: prevSeq + 1},
		network,
		horizonbuild.CreateAccount(
			horizonbuild.Destination{AddressOrSeed: req.AccountId.Value},
			horizonbuild.NativeAmount{Amount: "0"},
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

	accountInfo, err := parseAccountInfo(req.AccountId, horizonAccount)
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
		horizonError, ok := err.(*horizonclient.Error)
		if ok && horizonError.Problem.Status == 404 {
			return &accountpb.GetAccountInfoResponse{Result: accountpb.GetAccountInfoResponse_NOT_FOUND}, nil
		} else {
			log.WithError(err).Warn("Failed to load account")
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	accountInfo, err := parseAccountInfo(req.AccountId, horizonAccount)
	if err != nil {
		log.WithError(err).Warn("Failed to parse account info from horizon account")
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &accountpb.GetAccountInfoResponse{
		Result:      accountpb.GetAccountInfoResponse_OK,
		AccountInfo: accountInfo,
	}, nil
}

// parseAccountInfo parses AccountInfo from an account fetched from Horizon
func parseAccountInfo(accountId *commonpb.StellarAccountId, horizonAccount horizonprotocols.Account) (info *accountpb.AccountInfo, err error) {
	strBalance, err := horizonAccount.GetNativeBalance()
	if err != nil {
		return nil, err
	}

	balance, err := amount.ParseInt64(strBalance)
	if err != nil {
		return nil, err
	}

	sequence, err := strconv.ParseInt(horizonAccount.Sequence, 10, 64)
	if err != nil {
		return nil, err
	}

	return &accountpb.AccountInfo{
		AccountId:      accountId,
		SequenceNumber: sequence,
		Balance:        balance,
	}, nil
}

// buildSignEncodeTransaction builds a transaction with the provided transaction mutators, signs it with the provided
// keypair and returns the base 64 XDR representation of the transaction envelope ready for submission to the network.
func buildSignEncodeTransaction(keypair *keypair.Full, muts ...horizonbuild.TransactionMutator) (string, error) {
	tx, err := horizonbuild.Transaction(muts...)
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
