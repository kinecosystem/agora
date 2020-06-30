package server

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis_rate/v8"
	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/agora-common/retry"
	"github.com/kinecosystem/agora-common/retry/backoff"
	"github.com/kinecosystem/go/build"
	"github.com/kinecosystem/go/clients/horizon"
	"github.com/kinecosystem/go/keypair"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go/xdr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v3"

	"github.com/kinecosystem/agora/pkg/channel"
)

const (
	eventStreamBufferSize = 64

	globalRateLimitKey = "create-account-rate-limit-global"
)

type server struct {
	log             *logrus.Entry
	rootAccountKP   *keypair.Full
	network         build.Network
	horizonClient   horizon.ClientInterface
	accountNotifier *AccountNotifier
	limiter         *redis_rate.Limiter
	channelPool     channel.Pool
	config          *Config
}

type Config struct {
	// CreateAccountGlobalLimit is the number of CreateAccount requests allowed globally per second.
	//
	// A value <= 0 indicates that no rate limit is to be applied.
	CreateAccountGlobalLimit int
}

// New returns a new account server
func New(
	rootAccountKP *keypair.Full,
	horizonClient horizon.ClientInterface,
	accountNotifier *AccountNotifier,
	limiter *redis_rate.Limiter,
	channelPool channel.Pool,
	c *Config,
) (accountpb.AccountServer, error) {
	network, err := kin.GetNetwork()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get network")
	}

	return &server{
		log:             logrus.StandardLogger().WithField("type", "account/server"),
		rootAccountKP:   rootAccountKP,
		network:         network,
		horizonClient:   horizonClient,
		accountNotifier: accountNotifier,
		limiter:         limiter,
		channelPool:     channelPool,
		config:          c,
	}, nil
}

// CreateAccount implements AccountServer.CreateAccount
func (s *server) CreateAccount(ctx context.Context, req *accountpb.CreateAccountRequest) (*accountpb.CreateAccountResponse, error) {
	log := s.log.WithField("method", "CreateAccount")

	if s.config.CreateAccountGlobalLimit > 0 {
		result, err := s.limiter.Allow(globalRateLimitKey, redis_rate.PerSecond(s.config.CreateAccountGlobalLimit))
		if err != nil {
			fmt.Println(err.Error())
			log.WithError(err).Warn("failed to check global rate limit")
		} else if !result.Allowed {
			return nil, status.Error(codes.Unavailable, "rate limited")
		}
	}

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

	var sourceAcc horizon.Account
	var sourceKP *keypair.Full
	if s.channelPool != nil {
		c, err := s.channelPool.GetChannel()
		if err != nil {
			log.WithError(err).Warn("Failed to get channelKP")
			return nil, status.Error(codes.Internal, err.Error())
		}
		defer func() {
			if err := c.Lock.Unlock(); err != nil {
				log.WithError(err).Warn("failed to unlock channel")
			}
		}()

		channelAccount, err := s.horizonClient.LoadAccount(c.KP.Address())
		if err != nil {
			horizonError, ok := err.(*horizon.Error)
			if !ok || horizonError.Problem.Status != 404 {
				log.WithError(err).Warn("Failed to check if channel account exists")
				return nil, status.Error(codes.Internal, "failed to create account")
			}

			rootHorizonAccount, err := s.horizonClient.LoadAccount(s.rootAccountKP.Address())
			if err != nil {
				log.WithError(err).Warn("Failed to load root account")
				return nil, status.Error(codes.Internal, err.Error())
			}

			_, err = retry.Retry(
				func() error {
					return s.createAccount(s.rootAccountKP, rootHorizonAccount, nil, c.KP.Address())
				},
				retry.Limit(3),
				retry.BackoffWithJitter(backoff.BinaryExponential(500*time.Millisecond), 2200*time.Millisecond, 0.1),
			)
			if err != nil {
				log.WithError(err).Warn("Failed to create channel account")
				return nil, status.Error(codes.Internal, "Failed to create account")
			}

			channelAccount, err = s.horizonClient.LoadAccount(c.KP.Address())
			if err != nil {
				log.WithError(err).Warn("Failed to load created/existing channel account")
				return nil, status.Error(codes.Internal, "Failed to create account")
			}
		}

		sourceAcc = channelAccount
		sourceKP = c.KP
	} else {
		rootHorizonAccount, err := s.horizonClient.LoadAccount(s.rootAccountKP.Address())
		if err != nil {
			log.WithError(err).Warn("Failed to load root account")
			return nil, status.Error(codes.Internal, err.Error())
		}

		sourceAcc = rootHorizonAccount
		sourceKP = s.rootAccountKP
	}

	err = s.createAccount(sourceKP, sourceAcc, s.rootAccountKP, req.AccountId.Value)
	if err != nil {
		log.WithError(err).Warn("Failed to create new account")
		return nil, status.Error(codes.Internal, "Failed to create account")
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

	log = log.WithField("account", req.AccountId.Value)
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
		as.close()
		s.accountNotifier.RemoveStream(req.AccountId.Value, as)
	}()

	events := make([]*accountpb.Event, 0)
	for {
		select {
		case xdrData, ok := <-as.streamCh:
			if !ok {
				return status.Error(codes.Aborted, "")
			}

			envBytes, err := xdrData.Envelope.MarshalBinary()
			if err != nil {
				log.WithError(err).Warn("failed to marshal transaction envelope, dropping transaction")
				break
			}

			resultBytes, err := xdrData.Result.MarshalBinary()
			if err != nil {
				log.WithError(err).Warn("failed to marshal transaction result, dropping transaction")
				break
			}

			events = append(events, &accountpb.Event{
				Type: &accountpb.Event_TransactionEvent{
					TransactionEvent: &accountpb.TransactionEvent{
						EnvelopeXdr: envBytes,
						ResultXdr:   resultBytes,
					},
				},
			})

			accountInfo, accountRemoved, err := s.getMetaAccountInfoOrLoad(req.AccountId.Value, xdrData.Meta)
			if err != nil {
				log.WithError(err).Warn("failed to get account info, excluding account event")
			} else if accountInfo != nil {
				events = append(events, &accountpb.Event{
					Type: &accountpb.Event_AccountUpdateEvent{
						AccountUpdateEvent: &accountpb.AccountUpdateEvent{
							AccountInfo: accountInfo,
						},
					},
				})
			}

			// The max # of events that can be sent is 128 and each xdrData received from streamCh results in up to 2
			// events, so we should flush at a length >= 127.
			if len(events) >= 127 || accountRemoved || len(as.streamCh) == 0 {
				err = stream.Send(&accountpb.Events{
					Events: events,
				})
				if err != nil {
					log.WithError(err).Info("failed to send events")
					return err
				}
				if accountRemoved {
					return nil
				}
				events = make([]*accountpb.Event, 0)
			}
		case <-stream.Context().Done():
			log.Debug("Stream context cancelled, ending stream")
			return status.Error(codes.Canceled, "")
		}
	}
}

// getMetaAccountInfoOrLoad attempts to parse account info from the provided TransactionMeta. If the account info is
// not present in the TransactionMeta, it will attempt to fetch the account info from Horizon.
func (s *server) getMetaAccountInfoOrLoad(accountID string, m xdr.TransactionMeta) (accountInfo *accountpb.AccountInfo, accountRemoved bool, err error) {
	log := s.log.WithField("method", "getMetaAccountInfoOrLoad")

	for _, opMeta := range m.OperationsMeta() {
		for _, lec := range opMeta.Changes {
			switch lec.Type {
			case xdr.LedgerEntryChangeTypeLedgerEntryCreated, xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
				entry, ok := lec.GetLedgerEntry()
				if !ok {
					log.Warnf("ledger entry not present in ledger entry change of type %d", lec.Type)
				}

				if entry.Data.Type == xdr.LedgerEntryTypeAccount {
					account, ok := entry.Data.GetAccount()
					if !ok {
						log.Warn("account not present in account ledger entry data")
					}

					if account.AccountId.Address() == accountID {
						info, err := parseAccountInfoFromEntry(account)
						if err != nil {
							log.WithError(err).Warn("failed to parse account info from account entry")
						}

						accountInfo = info
					}
				}
			case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
				ledgerKey := lec.Removed
				if ledgerKey != nil {
					if ledgerKey.Type == xdr.LedgerEntryTypeAccount {
						accountKey, ok := ledgerKey.GetAccount()
						if !ok {
							log.Warn("account key not present in account ledger key")
						}

						if accountKey.AccountId.Address() == accountID {
							accountRemoved = true
						}
					}
				}
			}
		}
	}

	if accountInfo == nil && !accountRemoved {
		account, err := s.horizonClient.LoadAccount(accountID)
		if err != nil {
			return nil, false, err
		}

		accountInfo, err = parseAccountInfo(account)
		if err
		!= nil {
			return nil, false, err
		}
	}

	return accountInfo, accountRemoved, err
}

func (s *server) createAccount(sourceKP *keypair.Full, sourceAccount horizon.Account, whitelistingKP *keypair.Full, newAccountID string) (err error) {
	// sequence number is typically represented with an int64, but the Horizon client sequence number mutator uses a
	// uint64, so we parse it accordingly here.
	prevSeq, err := strconv.ParseUint(sourceAccount.Sequence, 10, 64)
	if err != nil {
		return errors.Wrap(err, "failed to parse root account sequence number")
	}

	tx, err := build.Transaction(build.SourceAccount{AddressOrSeed: sourceKP.Address()},
		build.Sequence{Sequence: prevSeq + 1},
		s.network,
		build.CreateAccount(
			build.Destination{AddressOrSeed: newAccountID},
			build.NativeAmount{Amount: "0"},
		),
	)
	if err != nil {
		return errors.Wrap(err, "failed to build transaction")
	}

	signers := []string{sourceKP.Seed()}
	if whitelistingKP != nil {
		signers = append(signers, whitelistingKP.Seed())
	}

	signedTx, err := tx.Sign(signers...)
	if err != nil {
		return errors.Wrap(err, "failed to sign transaction")
	}

	encodedTx, err := signedTx.Base64()
	if err != nil {
		return errors.Wrap(err, "failed to encode transaction")
	}

	_, err = s.horizonClient.SubmitTransaction(encodedTx)
	if err != nil {
		hErr, ok := err.(*horizon.Error)
		if !ok {
			return errors.Wrap(err, "failed to submit create account transaction")
		}

		rc, err := hErr.ResultCodes()
		if err != nil {
			return errors.Wrap(hErr, "failed to get result codes from horizon error")
		}

		if len(rc.OperationCodes) == 0 || rc.OperationCodes[0] != "op_already_exists" {
			return errors.Wrap(hErr, "failed to submit create account transaction")
		}
	}

	return nil
}
