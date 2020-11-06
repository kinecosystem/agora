package solana

import (
	"bytes"
	"context"
	"crypto/ed25519"

	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/system"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/mr-tron/base58"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go/strkey"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v4"
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v4"

	"github.com/kinecosystem/agora/pkg/account"
	"github.com/kinecosystem/agora/pkg/solanautil"
)

const (
	eventStreamBufferSize = 64
)

type server struct {
	log             *logrus.Entry
	sc              solana.Client
	tc              *token.Client
	limiter         *account.Limiter
	accountNotifier *AccountNotifier

	token              ed25519.PublicKey
	subsidizer         ed25519.PrivateKey
	minAccountLamports uint64
}

func New(
	sc solana.Client,
	limiter *account.Limiter,
	accountNotifier *AccountNotifier,
	mint ed25519.PublicKey,
	subsidizer ed25519.PrivateKey,
) (accountpb.AccountServer, error) {
	s := &server{
		log:             logrus.StandardLogger().WithField("type", "account/solana"),
		sc:              sc,
		tc:              token.NewClient(sc, mint),
		accountNotifier: accountNotifier,
		limiter:         limiter,
		token:           mint,
		subsidizer:      subsidizer,
	}

	minAccountLamports, err := sc.GetMinimumBalanceForRentExemption(token.AccountSize)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get minimum balance for rent exemption")
	}

	s.minAccountLamports = minAccountLamports

	return s, nil
}

func (s *server) CreateAccount(ctx context.Context, req *accountpb.CreateAccountRequest) (*accountpb.CreateAccountResponse, error) {
	log := s.log.WithField("method", "CreateAccount")

	var txn solana.Transaction
	if err := txn.Unmarshal(req.Transaction.Value); err != nil {
		log.WithError(err).Debug("bad transaction encoding")
		return nil, status.Error(codes.InvalidArgument, "bad transaction encoding")
	}

	if len(s.subsidizer) == 0 {
		if len(txn.Message.Instructions) != 2 {
			return nil, status.Error(codes.InvalidArgument, "expected 2 instructions (Sys::CreateAccount, Token::InitializeAccount)")
		}
	} else {
		if len(txn.Message.Instructions) != 3 {
			return nil, status.Error(codes.InvalidArgument, "expected 3 instructions (Sys::CreateAccount, Token::InitializeAccount)")
		}

	}

	allowed, err := s.limiter.Allow(4)
	if err != nil {
		log.WithError(err).Warn("failed to check rate limit")
	} else if !allowed {
		return nil, status.Error(codes.ResourceExhausted, "rate limited")
	}

	//
	// Validate System::Create command
	//
	sysCreate, err := system.DecompileCreateAccount(txn.Message, 0)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid Sys::CreateAccount instruction")
	}
	if sysCreate.Size != token.AccountSize {
		return nil, status.Errorf(codes.InvalidArgument, "invalid account size. expected %d", token.AccountSize)
	}
	if !bytes.Equal(sysCreate.Owner, token.ProgramKey) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid account owner. expected %s", base58.Encode(token.ProgramKey))
	}
	if sysCreate.Lamports != s.minAccountLamports {
		return nil, status.Errorf(codes.InvalidArgument, "invalid amount of lamports. expected: %d", s.minAccountLamports)
	}

	//
	// Validate Token::InitializeAccount command
	//
	tokenInitialize, err := token.DecompileInitializeAccount(txn.Message, 1)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid Token::InitializeAccount instruction")
	}
	if !bytes.Equal(tokenInitialize.Account, sysCreate.Address) {
		return nil, status.Error(codes.InvalidArgument, "different accounts in Sys::CreateAccount and Token::InitializeAccount")
	}
	if !bytes.Equal(tokenInitialize.Mint, s.token) {
		return nil, status.Error(codes.InvalidArgument, "Token::InitializeAccount has incorrect mint")
	}

	//
	// Validate Token::SetAuthority command
	//
	if len(s.subsidizer) > 0 {
		setAuthority, err := token.DecompileSetAuthority(txn.Message, 2)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid Token::SetAuthority instruction: %v", err)
		}
		if setAuthority.Type != token.AuthorityTypeCloseAccount {
			return nil, status.Errorf(codes.InvalidArgument, "Token::SetAuthority must be for CloseAuthority: %v", err)
		}
		if !bytes.Equal(setAuthority.Account, sysCreate.Address) {
			return nil, status.Errorf(codes.InvalidArgument, "close authority is not for the created account: %v", err)
		}
		if !bytes.Equal(setAuthority.NewAuthority, s.subsidizer.Public().(ed25519.PublicKey)) {
			return nil, status.Error(codes.InvalidArgument, "close authority is not the subsidizer")
		}
	}

	// todo: extract to be function check
	if len(s.subsidizer) > 0 && bytes.Equal(txn.Message.Accounts[0], s.subsidizer.Public().(ed25519.PublicKey)) {
		if err := txn.Sign(s.subsidizer); err != nil {
			return nil, status.Error(codes.Internal, "failed to co-sign txn")
		}
	}

	_, stat, err := s.sc.SubmitTransaction(txn, solanautil.CommitmentFromProto(req.Commitment))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "unhandled error from SubmitTransaction: %v", err)
	}
	if stat.ErrorResult != nil {
		if solanautil.IsAccountAlreadyExistsError(stat.ErrorResult) {
			return &accountpb.CreateAccountResponse{
				Result: accountpb.CreateAccountResponse_EXISTS,
			}, nil
		}
		if stat.ErrorResult.ErrorKey() == solana.TransactionErrorBlockhashNotFound {
			return &accountpb.CreateAccountResponse{
				Result: accountpb.CreateAccountResponse_BAD_NONCE,
			}, nil
		}

		log.WithError(err).Warn("unexpected transaction error")
		return nil, status.Errorf(codes.Internal, "unhandled error from SubmitTransaction: %v", stat.ErrorResult)
	}

	return &accountpb.CreateAccountResponse{
		Result: accountpb.CreateAccountResponse_OK,
		AccountInfo: &accountpb.AccountInfo{
			AccountId: &commonpb.SolanaAccountId{
				Value: tokenInitialize.Account,
			},
			Balance: 0,
		},
	}, nil
}

func (s *server) GetAccountInfo(_ context.Context, req *accountpb.GetAccountInfoRequest) (*accountpb.GetAccountInfoResponse, error) {
	account, err := s.tc.GetAccount(ed25519.PublicKey(req.AccountId.Value), solanautil.CommitmentFromProto(req.Commitment))
	if err == token.ErrInvalidTokenAccount || err == token.ErrAccountNotFound {
		return &accountpb.GetAccountInfoResponse{
			Result: accountpb.GetAccountInfoResponse_NOT_FOUND,
		}, nil
	} else if err != nil {
		return nil, status.Error(codes.Internal, "failed to retrieve account info")
	}

	return &accountpb.GetAccountInfoResponse{
		AccountInfo: &accountpb.AccountInfo{
			AccountId: req.AccountId,
			Balance:   int64(account.Amount),
		},
	}, nil
}

func (s *server) ResolveTokenAccounts(_ context.Context, req *accountpb.ResolveTokenAccountsRequest) (*accountpb.ResolveTokenAccountsResponse, error) {
	accounts, err := s.sc.GetTokenAccountsByOwner(req.AccountId.Value, s.token)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	resp := &accountpb.ResolveTokenAccountsResponse{
		TokenAccounts: make([]*commonpb.SolanaAccountId, len(accounts)),
	}

	// The number of accounts by owner is likely to be small, so this is fairly quick.
	// If we find that the identity address is in the list, then we place it at the start,
	// and shuffle as follows:
	//
	// Already in place:
	//  [X A A A B B B B B]
	//  [X A A A B B B B B]
	//
	// Mid list:
	//  [A A A A X B B B B B]
	//    \ \ \ \  | | | | |
	//  [X A A A A B B B B B]
	//
	// End of list:
	//  [A A A A B B B B B X]
	//    \ \ \ \ \ \ \ \ \
	//  [X A A A A B B B B B]
	//
	// when i < X, we insert at i+1
	// when i = X, we skip
	// when i > X, we insert at i
	//
	offset, identityIndex := 0, -1
	for i := range accounts {
		if bytes.Equal(accounts[i], req.AccountId.Value) {
			identityIndex = i
			offset = 1
			break
		}
	}

	if identityIndex >= 0 {
		resp.TokenAccounts[0] = &commonpb.SolanaAccountId{
			Value: accounts[identityIndex],
		}
	}
	for i := range accounts {
		if i == identityIndex {
			offset--
			continue
		}

		resp.TokenAccounts[i+offset] = &commonpb.SolanaAccountId{
			Value: accounts[i],
		}
	}

	return resp, nil
}

func (s *server) GetEvents(req *accountpb.GetEventsRequest, stream accountpb.Account_GetEventsServer) error {
	log := s.log.WithField("method", "GetEvents")

	account, err := s.tc.GetAccount(req.AccountId.Value, solana.CommitmentRecent)
	if err == token.ErrAccountNotFound || err == token.ErrInvalidTokenAccount {
		if err := stream.Send(&accountpb.Events{Result: accountpb.Events_NOT_FOUND}); err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		return nil
	} else if err != nil {
		log.WithError(err).Warn("failed to load account")
		return status.Error(codes.Internal, err.Error())
	}

	log = log.WithField("account", base58.Encode(req.AccountId.Value))

	err = stream.Send(&accountpb.Events{
		Events: []*accountpb.Event{
			{
				Type: &accountpb.Event_AccountUpdateEvent{
					AccountUpdateEvent: &accountpb.AccountUpdateEvent{
						AccountInfo: &accountpb.AccountInfo{
							AccountId: req.AccountId,
							Balance:   int64(account.Amount),
						},
					},
				},
			},
		},
	})
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	accountKey, err := strkey.Encode(strkey.VersionByteAccountID, req.AccountId.Value)
	if err != nil {
		return status.Error(codes.InvalidArgument, "invalid key")
	}

	as := newEventStream(eventStreamBufferSize)
	s.accountNotifier.AddStream(accountKey, as)

	defer func() {
		as.close()
		s.accountNotifier.RemoveStream(accountKey, as)
	}()

	events := make([]*accountpb.Event, 0)
	for {
		select {
		case txn, ok := <-as.streamCh:
			if !ok {
				return status.Error(codes.Aborted, "")
			}

			var txErr *commonpb.TransactionError
			if txn.Err != nil {
				txErr, err = solanautil.MapTransactionError(*txn.Err)
				if err != nil {
					log.WithError(err).Warn("failed to map transaction error, dropping")
					continue
				}
			}

			events = append(events, &accountpb.Event{
				Type: &accountpb.Event_TransactionEvent{
					TransactionEvent: &accountpb.TransactionEvent{
						Transaction: &commonpb.Transaction{
							Value: txn.Transaction.Marshal(),
						},
						TransactionError: txErr,
					},
				},
			})

			accountInfo, err := s.tc.GetAccount(req.AccountId.Value, solana.CommitmentRecent)
			if err != nil {
				log.WithError(err).Warn("failed to get account info, excluding account event")
			} else {
				events = append(events, &accountpb.Event{
					Type: &accountpb.Event_AccountUpdateEvent{
						AccountUpdateEvent: &accountpb.AccountUpdateEvent{
							AccountInfo: &accountpb.AccountInfo{
								AccountId: req.AccountId,
								Balance:   int64(accountInfo.Amount),
							},
						},
					},
				})
			}

			// The max # of events that can be sent is 128 and each xdrData received from streamCh results in up to 2
			// events, so we should flush at a length >= 127.
			if len(events) >= 127 || len(as.streamCh) == 0 {
				err = stream.Send(&accountpb.Events{
					Events: events,
				})
				if err != nil {
					log.WithError(err).Info("failed to send events")
					return err
				}
				events = make([]*accountpb.Event, 0)
			}
		case <-stream.Context().Done():
			log.Debug("Stream context cancelled, ending stream")
			return status.Error(codes.Canceled, "")
		}
	}
}