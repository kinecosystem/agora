package server

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/go-redis/redis_rate/v8"
	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/agora-common/retry"
	"github.com/kinecosystem/agora-common/retry/backoff"
	"github.com/kinecosystem/agora/pkg/version"
	"github.com/kinecosystem/go/amount"
	"github.com/kinecosystem/go/build"
	"github.com/kinecosystem/go/clients/horizon"
	"github.com/kinecosystem/go/keypair"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
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

	// Channels need more than 3 XLM + the 100 lumen fee to create an account with a balance of 2 XLM
	minKin2ChannelBalance = 3e7 + 100
)

var (
	createAccountRLCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "create_account_rate_limited_global",
		Help:      "Number of globally rate limited create account requests",
	})
)

type server struct {
	log *logrus.Entry

	rootKP          *keypair.Full
	network         build.Network
	client          horizon.ClientInterface
	accountNotifier *AccountNotifier
	channelPool     channel.Pool

	kin2RootKP          *keypair.Full
	kin2Network         build.Network
	kin2Client          horizon.ClientInterface
	kin2Issuer          string
	kin2AccountNotifier *AccountNotifier
	kin2ChannelPool     channel.Pool

	limiter *redis_rate.Limiter
	config  *Config
}

type Config struct {
	// CreateAccountGlobalLimit is the number of CreateAccount requests allowed globally per second.
	//
	// A value <= 0 indicates that no rate limit is to be applied.
	CreateAccountGlobalLimit int
}

// New returns a new account server
func New(
	rootKP *keypair.Full,
	client horizon.ClientInterface,
	accountNotifier *AccountNotifier,
	channelPool channel.Pool,
	kin2RootKP *keypair.Full,
	kin2Client horizon.ClientInterface,
	kin2AccountNotifier *AccountNotifier,
	kin2ChannelPool channel.Pool,
	limiter *redis_rate.Limiter,
	c *Config,
) (accountpb.AccountServer, error) {
	network, err := kin.GetNetwork()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get network")
	}

	kin2Network, err := kin.GetKin2Network()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get kin 2 network")
	}

	kin2Issuer, err := kin.GetKin2Issuer()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get kin 2 issuer")
	}

	if err = registerMetrics(); err != nil {
		return nil, err
	}

	return &server{
		log:                 logrus.StandardLogger().WithField("type", "account/server"),
		rootKP:              rootKP,
		network:             network,
		client:              client,
		accountNotifier:     accountNotifier,
		channelPool:         channelPool,
		kin2RootKP:          kin2RootKP,
		kin2Network:         kin2Network,
		kin2Client:          kin2Client,
		kin2Issuer:          kin2Issuer,
		kin2AccountNotifier: kin2AccountNotifier,
		kin2ChannelPool:     kin2ChannelPool,
		limiter:             limiter,
		config:              c,
	}, nil
}

// CreateAccount implements AccountServer.CreateAccount
func (s *server) CreateAccount(ctx context.Context, req *accountpb.CreateAccountRequest) (*accountpb.CreateAccountResponse, error) {
	log := s.log.WithField("method", "CreateAccount")

	kinVersion, err := version.GetCtxKinVersion(ctx)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	var client horizon.ClientInterface
	var rootKP *keypair.Full
	var channelPool channel.Pool
	switch kinVersion {
	case version.KinVersion2:
		client = s.kin2Client
		rootKP = s.kin2RootKP
		channelPool = s.kin2ChannelPool
	default:
		client = s.client
		rootKP = s.rootKP
		channelPool = s.channelPool
	}

	if s.config.CreateAccountGlobalLimit > 0 {
		result, err := s.limiter.Allow(globalRateLimitKey, redis_rate.PerSecond(s.config.CreateAccountGlobalLimit))
		if err != nil {
			log.WithError(err).Warn("failed to check global rate limit")
		} else if !result.Allowed {
			createAccountRLCounter.Inc()
			return nil, status.Error(codes.Unavailable, "rate limited")
		}
	}

	// Check if account exists on the blockchain
	horizonAccount, err := client.LoadAccount(req.AccountId.Value)
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
	if channelPool != nil {
		c, err := channelPool.GetChannel()
		if err != nil {
			log.WithError(err).Warn("Failed to get channelKP")
			return nil, status.Error(codes.Internal, err.Error())
		}
		defer func() {
			if err := c.Lock.Unlock(); err != nil {
				log.WithError(err).Warn("failed to unlock channel")
			}
		}()

		var rootHorizonAccount horizon.Account

		channelAccount, err := client.LoadAccount(c.KP.Address())
		if err != nil {
			horizonError, ok := err.(*horizon.Error)
			if !ok || horizonError.Problem.Status != 404 {
				log.WithError(err).Warn("Failed to check if channel account exists")
				return nil, status.Error(codes.Internal, "failed to create account")
			}

			rootHorizonAccount, err = client.LoadAccount(rootKP.Address())
			if err != nil {
				log.WithError(err).Warn("Failed to load root account")
				return nil, status.Error(codes.Internal, err.Error())
			}

			_, err = retry.Retry(
				func() error {
					return s.createAccount(kinVersion, rootKP, rootHorizonAccount, nil, c.KP.Address())
				},
				retry.Limit(3),
				retry.BackoffWithJitter(backoff.BinaryExponential(500*time.Millisecond), 2200*time.Millisecond, 0.1),
			)
			if err != nil {
				if hErr, ok := err.(*horizon.Error); ok {
					resultXDR, envelopeXDR, err := parseXDRFromHorizonError(hErr)
					if err != nil {
						log.WithError(err).Warn("failed to parse XDR from horizon error")
					} else {
						log = log.WithFields(map[string]interface{}{
							"result_xdr":   resultXDR,
							"envelope_xdr": envelopeXDR,
						})
					}
				}

				log.WithError(err).Warn("Failed to create channel account")
				return nil, status.Error(codes.Internal, "Failed to create account")
			}

			channelAccount, err = client.LoadAccount(c.KP.Address())
			if err != nil {
				log.WithError(err).Warn("Failed to load created/existing channel account")
				return nil, status.Error(codes.Internal, "Failed to create account")
			}
		}

		if kinVersion == 2 {
			nativeBalance, err := channelAccount.GetNativeBalance()
			if err != nil {
				log.WithError(err).Warn("failed to get channel native balance")
				return nil, status.Error(codes.Internal, "Failed to create account")
			}
			channelBalance, err := amount.ParseInt64(nativeBalance)
			if err != nil {
				log.WithError(err).Warn("failed to parse channel native balance")
				return nil, status.Error(codes.Internal, "Failed to create account")
			}

			if channelBalance < minKin2ChannelBalance {
				if rootHorizonAccount.AccountID == "" {
					// load the root account, since we haven't already
					rootHorizonAccount, err = client.LoadAccount(rootKP.Address())
					if err != nil {
						log.WithError(err).Warn("Failed to load root account")
						return nil, status.Error(codes.Internal, err.Error())
					}
				}

				err = s.fundKin2Channel(rootHorizonAccount, c.KP.Address())
				if err != nil {
					if hErr, ok := err.(*horizon.Error); ok {
						resultXDR, envelopeXDR, err := parseXDRFromHorizonError(hErr)
						if err != nil {
							log.WithError(err).Warn("failed to parse XDR from horizon error")
						} else {
							log = log.WithFields(map[string]interface{}{
								"result_xdr":   resultXDR,
								"envelope_xdr": envelopeXDR,
							})
						}
					}

					log.WithError(err).Warn("Failed to fund channel account")
					return nil, status.Error(codes.Internal, "Failed to create account")
				}
			}
		}

		sourceAcc = channelAccount
		sourceKP = c.KP
	} else {
		rootHorizonAccount, err := client.LoadAccount(rootKP.Address())
		if err != nil {
			log.WithError(err).Warn("Failed to load root account")
			return nil, status.Error(codes.Internal, err.Error())
		}

		sourceAcc = rootHorizonAccount
		sourceKP = rootKP
	}

	err = s.createAccount(kinVersion, sourceKP, sourceAcc, rootKP, req.AccountId.Value)
	if err != nil {
		if hErr, ok := err.(*horizon.Error); ok {
			resultXDR, envelopeXDR, err := parseXDRFromHorizonError(hErr)
			if err != nil {
				log.WithError(err).Warn("failed to parse XDR from horizon error")
			} else {
				log = log.WithFields(map[string]interface{}{
					"result_xdr":   resultXDR,
					"envelope_xdr": envelopeXDR,
				})
			}
		}

		log.WithError(err).Warn("Failed to create new account")
		return nil, status.Error(codes.Internal, "Failed to create account")
	}

	horizonAccount, err = client.LoadAccount(req.AccountId.Value)
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

	kinVersion, err := version.GetCtxKinVersion(ctx)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	var client horizon.ClientInterface
	switch kinVersion {
	case version.KinVersion2:
		client = s.kin2Client
	default:
		client = s.client
	}

	horizonAccount, err := client.LoadAccount(req.AccountId.Value)
	if err != nil {
		horizonError, ok := err.(*horizon.Error)
		if ok && horizonError.Problem.Status == 404 {
			return &accountpb.GetAccountInfoResponse{Result: accountpb.GetAccountInfoResponse_NOT_FOUND}, nil
		}

		log.WithError(err).Warn("Failed to load account")
		return nil, status.Error(codes.Internal, err.Error())
	}

	var accountInfo *accountpb.AccountInfo
	switch kinVersion {
	case version.KinVersion2:
		accountInfo, err = parseKin2AccountInfo(horizonAccount, s.kin2Issuer)
	default:
		accountInfo, err = parseAccountInfo(horizonAccount)
	}
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

	kinVersion, err := version.GetCtxKinVersion(stream.Context())
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	var client horizon.ClientInterface
	var notifier *AccountNotifier
	switch kinVersion {
	case version.KinVersion2:
		client = s.kin2Client
		notifier = s.kin2AccountNotifier
	default:
		client = s.client
		notifier = s.accountNotifier
	}

	horizonAccount, err := client.LoadAccount(req.AccountId.Value)
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

	var accountInfo *accountpb.AccountInfo
	switch kinVersion {
	case version.KinVersion2:
		accountInfo, err = parseKin2AccountInfo(horizonAccount, s.kin2Issuer)
	default:
		accountInfo, err = parseAccountInfo(horizonAccount)
	}

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
	notifier.AddStream(req.AccountId.Value, as)

	defer func() {
		as.close()
		notifier.RemoveStream(req.AccountId.Value, as)
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

			accountInfo, accountRemoved, err := s.getMetaAccountInfoOrLoad(client, kinVersion, req.AccountId.Value, xdrData.Meta)
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
func (s *server) getMetaAccountInfoOrLoad(client horizon.ClientInterface, kinVersion version.KinVersion, accountID string, m xdr.TransactionMeta) (accountInfo *accountpb.AccountInfo, accountRemoved bool, err error) {
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
					} else if account.AccountId.Address() == accountID {
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
		account, err := client.LoadAccount(accountID)
		if err != nil {
			return nil, false, err
		}

		switch kinVersion {
		case version.KinVersion2:
			accountInfo, err = parseKin2AccountInfo(account, s.kin2Issuer)
		default:
			accountInfo, err = parseAccountInfo(account)
		}
		if err != nil {
			return nil, false, err
		}
	}

	return accountInfo, accountRemoved, err
}

func (s *server) createAccount(kinVersion version.KinVersion, sourceKP *keypair.Full, sourceAccount horizon.Account, whitelistingKP *keypair.Full, newAccountID string) (err error) {
	var network build.Network
	var client horizon.ClientInterface
	switch kinVersion {
	case version.KinVersion2:
		network = s.kin2Network
		client = s.kin2Client
	default:
		network = s.network
		client = s.client
	}

	// sequence number is typically represented with an int64, but the Horizon client sequence number mutator uses a
	// uint64, so we parse it accordingly here.
	prevSeq, err := strconv.ParseUint(sourceAccount.Sequence, 10, 64)
	if err != nil {
		return errors.Wrap(err, "failed to parse root account sequence number")
	}

	mutators := []build.TransactionMutator{
		build.SourceAccount{AddressOrSeed: sourceKP.Address()},
		build.Sequence{Sequence: prevSeq + 1},
		network,
	}
	if kinVersion == 2 {
		mutators = append(mutators,
			build.CreateAccount(
				build.Destination{AddressOrSeed: newAccountID},
				build.NativeAmount{Amount: "200"}, // 2 XLM, the minimum amount of XLM an account must hold
			),
			build.Trust(kin.KinAssetCode, s.kin2Issuer, build.SourceAccount{AddressOrSeed: newAccountID}),
		)
	} else {
		mutators = append(mutators,
			build.CreateAccount(
				build.Destination{AddressOrSeed: newAccountID},
				build.NativeAmount{Amount: "0"},
			),
		)
	}

	tx, err := build.Transaction(mutators...)
	if err != nil {
		return errors.Wrap(err, "failed to build transaction")
	}

	signers := []string{sourceKP.Seed()}
	if whitelistingKP != nil && whitelistingKP.Seed() != sourceKP.Seed() {
		signers = append(signers, whitelistingKP.Seed())
	}

	err = signAndSubmitTransaction(client, tx, signers)
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
			return hErr
		}
	}
	return nil
}

func (s *server) fundKin2Channel(rootAccount horizon.Account, channelAccountID string) (err error) {
	// sequence number is typically represented with an int64, but the Horizon client sequence number mutator uses a
	// uint64, so we parse it accordingly here.
	prevSeq, err := strconv.ParseUint(rootAccount.Sequence, 10, 64)
	if err != nil {
		return errors.Wrap(err, "failed to parse root account sequence number")
	}

	tx, err := build.Transaction(
		build.SourceAccount{AddressOrSeed: s.kin2RootKP.Address()},
		build.Sequence{Sequence: prevSeq + 1},
		s.kin2Network,
		build.Payment(
			build.Destination{AddressOrSeed: channelAccountID},
			build.NativeAmount{Amount: "10000"}, // 100 XLM
		),
	)
	if err != nil {
		return errors.Wrap(err, "failed to build transaction")
	}

	return signAndSubmitTransaction(s.kin2Client, tx, []string{s.kin2RootKP.Seed()})
}

func signAndSubmitTransaction(client horizon.ClientInterface, tx *build.TransactionBuilder, signers []string) (err error) {
	signedTx, err := tx.Sign(signers...)
	if err != nil {
		return errors.Wrap(err, "failed to sign transaction")
	}

	encodedTx, err := signedTx.Base64()
	if err != nil {
		return errors.Wrap(err, "failed to encode transaction")
	}

	_, err = client.SubmitTransaction(encodedTx)
	return err
}

func parseXDRFromHorizonError(hErr *horizon.Error) (resultXDR string, envelopeXDR string, err error) {
	if err := json.Unmarshal(hErr.Problem.Extras["result_xdr"], &resultXDR); err != nil {
		return resultXDR, envelopeXDR, errors.Wrap(hErr, "invalid json result encoding from horizon")
	}

	if err := json.Unmarshal(hErr.Problem.Extras["envelope_xdr"], &envelopeXDR); err != nil {
		return resultXDR, envelopeXDR, errors.Wrap(hErr, "invalid json envelope encoding from horizon")
	}

	return resultXDR, envelopeXDR, nil
}

func registerMetrics() (err error) {
	if err := prometheus.Register(createAccountRLCounter); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			createAccountRLCounter = e.ExistingCollector.(prometheus.Counter)
			return nil
		}
		return errors.Wrap(err, "failed to register create account global rate limit counter")
	}

	return nil
}
