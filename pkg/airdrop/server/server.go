package server

import (
	"context"
	"crypto/ed25519"

	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	airdroppb "github.com/kinecosystem/agora-api/genproto/airdrop/v4"
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v4"

	"github.com/kinecosystem/agora/pkg/solanautil"
)

const (
	maxAirdrop = 1000 * 1e5
)

type server struct {
	log         *logrus.Entry
	sc          solana.Client
	tc          *token.Client
	subsidizer  ed25519.PrivateKey
	source      ed25519.PublicKey
	sourceOwner ed25519.PrivateKey
}

func New(sc solana.Client, mint, source ed25519.PublicKey, subsidizer, sourceOwner ed25519.PrivateKey) airdroppb.AirdropServer {
	return &server{
		log:         logrus.StandardLogger().WithField("type", "airdrop/server"),
		sc:          sc,
		tc:          token.NewClient(sc, mint),
		subsidizer:  subsidizer,
		source:      source,
		sourceOwner: sourceOwner,
	}
}

// RequestAirdrop requests an air drop of kin to the target account.
func (s *server) RequestAirdrop(_ context.Context, req *airdroppb.RequestAirdropRequest) (*airdroppb.RequestAirdropResponse, error) {
	if req.Quarks > maxAirdrop {
		return nil, status.Error(codes.ResourceExhausted, "try requesting less :)")
	}

	source, err := s.tc.GetAccount(s.source, solanautil.CommitmentFromProto(req.Commitment))
	if err == token.ErrAccountNotFound {
		return &airdroppb.RequestAirdropResponse{Result: airdroppb.RequestAirdropResponse_INSUFFICIENT_KIN}, nil
	} else if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to lookup source account: %v", err)
	}

	if source.Amount < req.Quarks {
		return &airdroppb.RequestAirdropResponse{Result: airdroppb.RequestAirdropResponse_INSUFFICIENT_KIN}, nil
	}

	hash, err := s.sc.GetRecentBlockhash()
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to get recent block hash")
	}

	txn := solana.NewTransaction(
		s.subsidizer.Public().(ed25519.PublicKey),
		token.Transfer(
			s.source,
			ed25519.PublicKey(req.AccountId.Value),
			s.sourceOwner.Public().(ed25519.PublicKey),
			req.Quarks,
		),
	)
	txn.SetBlockhash(hash)
	if err := txn.Sign(s.sourceOwner); err != nil {
		return nil, status.Error(codes.Internal, "failed to co-sign transaction")
	}

	sig, stat, err := s.sc.SubmitTransaction(txn, solanautil.CommitmentFromProto(req.Commitment))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to submit transaction: %v", err.Error())
	}
	if stat.ErrorResult != nil {
		txErr, err := solanautil.MapTransactionError(*stat.ErrorResult)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to submit transaction: %v", stat.ErrorResult.Error())
		}

		switch txErr.Reason {
		case commonpb.TransactionError_INSUFFICIENT_FUNDS:
			return &airdroppb.RequestAirdropResponse{
				Result: airdroppb.RequestAirdropResponse_INSUFFICIENT_KIN,
			}, nil
		case commonpb.TransactionError_INVALID_ACCOUNT:
			return &airdroppb.RequestAirdropResponse{
				Result: airdroppb.RequestAirdropResponse_NOT_FOUND,
			}, nil
		default:
			return nil, status.Errorf(codes.Internal, "failed to submit transaction: %v", stat.ErrorResult.Error())
		}
	}

	return &airdroppb.RequestAirdropResponse{
		Result: airdroppb.RequestAirdropResponse_OK,
		Signature: &commonpb.TransactionSignature{
			Value: sig[:],
		},
	}, nil
}
