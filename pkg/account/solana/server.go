package solana

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"math/rand"
	"strings"

	"github.com/kinecosystem/agora-common/headers"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/system"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/kinecosystem/agora/client"
	"github.com/kinecosystem/go/amount"
	"github.com/kinecosystem/go/clients/horizon"
	"github.com/mr-tron/base58"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go/strkey"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v4"
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v4"

	"github.com/kinecosystem/agora/pkg/account"
	"github.com/kinecosystem/agora/pkg/account/solana/accountinfo"
	"github.com/kinecosystem/agora/pkg/account/solana/tokenaccount"
	"github.com/kinecosystem/agora/pkg/migration"
	"github.com/kinecosystem/agora/pkg/solanautil"
)

const (
	eventStreamBufferSize = 64
)

var (
	consistencyCheckFailedCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "token_account_cache_consistency_check_failures",
		Help:      "Number of token account cache consistency check failures",
	})
	resolveAccountHitCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "resolve_token_account_hits",
		Help:      "Number of times at least one token account was resolved for a requested account",
	})
	resolveAccountMissCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "resolve_token_account_misses",
		Help:      "Number of times no token account was resolved for a requested account",
	})

	createAccountBlockCounterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "create_account_blocks",
		Help:      "Number of blocked create account requests",
	}, []string{"kin_user_agent"})
)

type server struct {
	log               *logrus.Entry
	sc                solana.Client
	scSubmit          solana.Client
	tc                *token.Client
	hc                horizon.ClientInterface
	limiter           *account.Limiter
	accountNotifier   *AccountNotifier
	tokenAccountCache tokenaccount.Cache
	infoCache         accountinfo.Cache
	migrator          migration.Migrator
	migrationStore    migration.Store
	mapper            account.Mapper

	token              ed25519.PublicKey
	subsidizer         ed25519.PrivateKey
	minAccountLamports uint64

	cacheCheckProbability float32

	// If no secret is set, all requests will be whitelisted
	createWhitelistSecret string
}

func init() {
	if err := registerMetrics(); err != nil {
		logrus.WithError(err).Error("failed to register account server metrics")
	}
}

func New(
	sc solana.Client,
	scSubmit solana.Client,
	hc horizon.ClientInterface,
	limiter *account.Limiter,
	accountNotifier *AccountNotifier,
	tokenAccountCache tokenaccount.Cache,
	infoCache accountinfo.Cache,
	migrator migration.Migrator,
	migrationStore migration.Store,
	mapper account.Mapper,
	mint ed25519.PublicKey,
	subsidizer ed25519.PrivateKey,
	cacheCheckFreq float32,
	createWhitelistSecret string,
) (accountpb.AccountServer, error) {
	s := &server{
		log:                   logrus.StandardLogger().WithField("type", "account/solana"),
		sc:                    sc,
		scSubmit:              scSubmit,
		tc:                    token.NewClient(sc, mint),
		hc:                    hc,
		accountNotifier:       accountNotifier,
		tokenAccountCache:     tokenAccountCache,
		infoCache:             infoCache,
		migrator:              migrator,
		migrationStore:        migrationStore,
		mapper:                mapper,
		limiter:               limiter,
		token:                 mint,
		subsidizer:            subsidizer,
		cacheCheckProbability: cacheCheckFreq,
		createWhitelistSecret: createWhitelistSecret,
	}

	backupValue := uint64(2039280)
	minAccountLamports, err := sc.GetMinimumBalanceForRentExemption(token.AccountSize)
	if err != nil {
		s.log.WithError(err).Warn("Failed to load minimum balance for rent exemption, falling back.")
		s.minAccountLamports = backupValue
	} else {
		s.minAccountLamports = minAccountLamports
	}

	if minAccountLamports != backupValue {
		s.log.WithFields(logrus.Fields{
			"actual": minAccountLamports,
			"backup": backupValue,
		}).Warn("backup value does not match actual")
	}

	return s, nil
}

func (s *server) CreateAccount(ctx context.Context, req *accountpb.CreateAccountRequest) (*accountpb.CreateAccountResponse, error) {
	log := s.log.WithField("method", "CreateAccount")

	if ua, ok := s.isWhitelisted(ctx); !ok {
		createAccountBlockCounterVec.WithLabelValues(ua).Inc()
		return nil, status.Error(codes.ResourceExhausted, "rate limited")
	}

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

	if err := s.mapper.Add(ctx, tokenInitialize.Account, tokenInitialize.Owner); err != nil {
		log.WithError(err).Warn("failed to store account mapping")
		return nil, status.Error(codes.Internal, "failed to store account mapping")
	}

	_, stat, err := s.scSubmit.SubmitTransaction(txn, solanautil.CommitmentFromProto(req.Commitment))
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

func (s *server) GetAccountInfo(ctx context.Context, req *accountpb.GetAccountInfoRequest) (*accountpb.GetAccountInfoResponse, error) {
	log := s.log.WithField("method", "GetAccountInfo")

	commitment := solanautil.CommitmentFromProto(req.Commitment)
	err := s.migrator.InitiateMigration(ctx, req.AccountId.Value, false, commitment)
	if err != nil && err != migration.ErrNotFound {
		log.WithError(err).Warn("failed to initiate migration")

		// Check to see if migration was started at all; fallback to Horizon if it wasn't
		_, exists, err := s.migrationStore.Get(ctx, req.AccountId.Value)
		if err != nil {
			log.WithError(err).Warn("failed to get migration status")
			return nil, status.Error(codes.Internal, "failed to get migration status")
		}
		if !exists {
			balance, err := s.loadHorizonBalance(req.AccountId.Value)
			if err != nil {
				return nil, status.Error(codes.Internal, "failed to get account balance")
			}

			return &accountpb.GetAccountInfoResponse{
				AccountInfo: &accountpb.AccountInfo{
					AccountId: &commonpb.SolanaAccountId{Value: req.AccountId.Value},
					Balance:   balance,
				},
			}, nil
		}
	}

	cached, err := s.infoCache.Get(ctx, req.AccountId.Value)
	if err != nil && err != accountinfo.ErrAccountInfoNotFound {
		log.WithError(err).Warn("failed to get account cached from cache")
	} else if cached != nil {
		// The balance can never actually be zero on chain.
		//
		// Negative balance is what we exploit to indicate we got a negative
		// result from a query.
		if cached.Balance < 0 {
			return &accountpb.GetAccountInfoResponse{
				Result: accountpb.GetAccountInfoResponse_NOT_FOUND,
			}, nil
		}

		return &accountpb.GetAccountInfoResponse{
			AccountInfo: cached,
		}, nil
	}

	account, err := s.tc.GetAccount(req.AccountId.Value, solanautil.CommitmentFromProto(req.Commitment))
	if err != nil && err != token.ErrInvalidTokenAccount && err != token.ErrAccountNotFound {
		return nil, status.Error(codes.Internal, "failed to retrieve account cached")
	}

	var info *accountpb.AccountInfo
	var resp *accountpb.GetAccountInfoResponse
	if err != nil {
		// account not found
		info = &accountpb.AccountInfo{
			AccountId: req.AccountId,
			Balance:   int64(-1), // sentinel value
		}
		resp = &accountpb.GetAccountInfoResponse{
			Result: accountpb.GetAccountInfoResponse_NOT_FOUND,
		}
	} else {
		info = &accountpb.AccountInfo{
			AccountId: req.AccountId,
			Balance:   int64(account.Amount),
		}
		resp = &accountpb.GetAccountInfoResponse{
			AccountInfo: info,
		}

		if err := s.mapper.Add(ctx, req.AccountId.Value, account.Owner); err != nil {
			log.WithError(err).Warn("failed to cache get account mapping")
		}
	}

	if err = s.infoCache.Put(ctx, info); err != nil {
		log.WithError(err).Warn("failed to cache get account info")
	}
	return resp, nil
}

func (s *server) ResolveTokenAccounts(ctx context.Context, req *accountpb.ResolveTokenAccountsRequest) (*accountpb.ResolveTokenAccountsResponse, error) {
	log := s.log.WithField("method", "ResolveTokenAccounts")

	var accounts []ed25519.PublicKey
	var putRequired bool

	cached, err := s.tokenAccountCache.Get(ctx, req.AccountId.Value)
	if err != nil && err != tokenaccount.ErrTokenAccountsNotFound {
		log.WithError(err).Warn("failed to get token accounts from cache")
	}
	if len(cached) == 0 || rand.Float32() < s.cacheCheckProbability {
		putRequired = true
		accounts, err = s.sc.GetTokenAccountsByOwner(req.AccountId.Value, s.token)
		if err != nil {
			log.WithError(err).Warn("failed to get token accounts")
			return nil, status.Error(codes.Internal, err.Error())
		}

		if len(cached) != 0 {
			checkCacheConsistency(cached, accounts)
		}
	} else {
		accounts = cached
	}

	if len(accounts) == 0 {
		// We only use recent here to ensure ResolveAccounts() is still fairly quick.
		if err := s.migrator.InitiateMigration(ctx, req.AccountId.Value, false, solana.CommitmentRecent); err != nil && err != migration.ErrNotFound {
			return nil, status.Errorf(codes.Internal, "failed to initiate migration: %v", err)
		}
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

	if putRequired {
		if len(resp.TokenAccounts) > 0 {
			keys := make([]ed25519.PublicKey, len(resp.TokenAccounts))
			for i, tokenAccount := range resp.TokenAccounts {
				keys[i] = tokenAccount.Value
				if err = s.mapper.Add(ctx, keys[i], req.AccountId.Value); err != nil {
					log.WithError(err).Warn("failed to add account mapping")
				}
			}
			if err = s.tokenAccountCache.Put(ctx, req.AccountId.Value, keys); err != nil {
				log.WithError(err).Warn("failed to cache token accounts")
			}
		} else {
			if err = s.tokenAccountCache.Delete(ctx, req.AccountId.Value); err != nil {
				log.WithError(err).Warn("failed to cache token accounts")
			}
		}
	}

	if len(resp.TokenAccounts) == 0 {
		resolveAccountMissCounter.Inc()
	} else {
		resolveAccountHitCounter.Inc()
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
				log.WithError(err).Warn("failed to add account info, excluding account event")
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

func (s *server) loadHorizonBalance(account ed25519.PublicKey) (int64, error) {
	address, err := strkey.Encode(strkey.VersionByteAccountID, account)
	if err != nil {
		return 0, errors.Wrap(err, "failed to encode account as stellar address")
	}
	stellarAccount, err := s.hc.LoadAccount(address)
	if err != nil {
		return 0, errors.Wrap(err, "failed to check kin3 account balance")
	}
	strBalance, err := stellarAccount.GetNativeBalance()
	if err != nil {
		return 0, errors.Wrap(err, "failed to get native balance")
	}
	balance, err := amount.ParseInt64(strBalance)
	if err != nil {
		return 0, errors.Wrap(err, "failed to parse balance")
	}
	if balance < 0 {
		return 0, errors.Errorf("cannot migrate negative balance: %d", balance)
	}

	return balance, nil
}

func (s *server) isWhitelisted(ctx context.Context) (userAgent string, whitelisted bool) {
	if len(s.createWhitelistSecret) == 0 {
		return "", true
	}

	val, err := headers.GetASCIIHeaderByName(ctx, client.UserAgentHeader)
	if err != nil {
		return val, false
	}

	if val == s.createWhitelistSecret {
		return val, true
	}

	if strings.Contains(val, "KinSDK/") && strings.Contains(val, "CID/") {
		return val, true
	}

	return val, false
}

func checkCacheConsistency(cached []ed25519.PublicKey, fetched []ed25519.PublicKey) {
	if len(cached) != len(fetched) {
		consistencyCheckFailedCounter.Inc()
	} else {
		keys := make(map[string]struct{})
		for _, a := range cached {
			keys[base58.Encode(a)] = struct{}{}
		}

		for _, a := range fetched {
			if _, ok := keys[base58.Encode(a)]; !ok {
				consistencyCheckFailedCounter.Inc()
				break
			}
		}
	}
}

func registerMetrics() (err error) {
	if err := prometheus.Register(consistencyCheckFailedCounter); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			consistencyCheckFailedCounter = e.ExistingCollector.(prometheus.Counter)
			return nil
		}
		return errors.Wrap(err, "failed to register token account cache consistency check failure counter")
	}

	if err := prometheus.Register(resolveAccountHitCounter); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			resolveAccountHitCounter = e.ExistingCollector.(prometheus.Counter)
		} else {
			return errors.Wrap(err, "failed to register resolve token account hit counter")
		}
	}
	if err := prometheus.Register(resolveAccountMissCounter); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			resolveAccountMissCounter = e.ExistingCollector.(prometheus.Counter)
		} else {
			return errors.Wrap(err, "failed to register resolve token account miss counter")
		}
	}

	if err := prometheus.Register(createAccountBlockCounterVec); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			createAccountBlockCounterVec = e.ExistingCollector.(*prometheus.CounterVec)
		} else {
			return errors.Wrap(err, "failed to register create account block counter vec")
		}
	}

	return nil
}
