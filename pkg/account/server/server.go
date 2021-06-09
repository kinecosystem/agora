package server

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/kinecosystem/agora-common/headers"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
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
	"github.com/kinecosystem/agora/pkg/account/info"
	"github.com/kinecosystem/agora/pkg/account/tokenaccount"
	"github.com/kinecosystem/agora/pkg/migration"
	"github.com/kinecosystem/agora/pkg/solanautil"
)

const (
	eventStreamBufferSize = 64
)

const (
	userAgentHeader = "kin-user-agent"
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
		Help:      "Number of token account cache hits",
	})
	resolveAccountMissCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "resolve_token_account_misses",
		Help:      "Number of token account cache misses",
	})
	nonMigratableResolveCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "non_migratable_resolves",
		Help:      "Number of times a resolve has been called for an account that cannot be migrated",
	})
	resolveShortCuts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "resolve_short_cuts",
		Help:      "Number short cuts taken for resolve token account",
	}, []string{"type"})

	createAccountFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "create_account_failures",
		Help:      "Number of create account failures",
	}, []string{"type"})
	createAccountByPlatform = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "create_account_by_platform",
		Help:      "Number of create account requests by 'platform' (mobile, other)",
	}, []string{"platform"})
	createAccountBlockCounterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "create_account_blocks",
		Help:      "Number of blocked create account requests",
	}, []string{"kin_user_agent"})
	createAccountResultCounterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "create_account_result",
		Help:      "Number of create account OKs by result",
	}, []string{"result"})
)

type sortableAccounts []ed25519.PublicKey

// Len is the number of elements in the collection.
func (s sortableAccounts) Len() int {
	return len(s)
}

// Less reports whether the element with
// index i should sort before the element with index j.
func (s sortableAccounts) Less(i int, j int) bool {
	return bytes.Compare(s[i], s[j]) < 1
}

// Swap swaps the elements with indexes i and j.
func (s sortableAccounts) Swap(i int, j int) {
	s[i], s[j] = s[j], s[i]
}

type server struct {
	log               *logrus.Entry
	conf              conf
	sc                solana.Client
	tc                *token.Client
	accountNotifier   *AccountNotifier
	tokenAccountCache tokenaccount.Cache
	infoCache         info.Cache
	loader            *info.Loader
	auth              account.Authorizer
	migrationLoader   migration.Loader
	migrator          migration.Migrator

	token              ed25519.PublicKey
	subsidizer         ed25519.PrivateKey
	minAccountLamports uint64

	// If no secret is set, all requests will be whitelisted
	createWhitelistSecret string
}

func init() {
	if err := registerMetrics(); err != nil {
		logrus.WithError(err).Error("failed to register account server metrics")
	}
}

func New(
	conf ConfigProvider,
	sc solana.Client,
	accountNotifier *AccountNotifier,
	tokenAccountCache tokenaccount.Cache,
	infoCache info.Cache,
	loader *info.Loader,
	authorizer account.Authorizer,
	migrationLoader migration.Loader,
	migrator migration.Migrator,
	mint ed25519.PublicKey,
	subsidizer ed25519.PrivateKey,
	createWhitelistSecret string,
	minAccountLamports uint64,
) accountpb.AccountServer {
	s := &server{
		log:                   logrus.StandardLogger().WithField("type", "account/server"),
		sc:                    sc,
		tc:                    token.NewClient(sc, mint),
		accountNotifier:       accountNotifier,
		tokenAccountCache:     tokenAccountCache,
		infoCache:             infoCache,
		loader:                loader,
		auth:                  authorizer,
		migrationLoader:       migrationLoader,
		migrator:              migrator,
		token:                 mint,
		subsidizer:            subsidizer,
		createWhitelistSecret: createWhitelistSecret,
		minAccountLamports:    minAccountLamports,
	}

	conf(&s.conf)

	return s
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

	auth, err := s.auth.Authorize(ctx, txn)
	if err != nil {
		return nil, err
	}
	if auth.Result == account.AuthorizationResultPayerRequired {
		return &accountpb.CreateAccountResponse{
			Result: accountpb.CreateAccountResponse_PAYER_REQUIRED,
		}, nil
	}

	info := &accountpb.AccountInfo{
		AccountId: &commonpb.SolanaAccountId{
			Value: auth.Address,
		},
		Owner: &commonpb.SolanaAccountId{
			Value: auth.Owner,
		},
		CloseAuthority: &commonpb.SolanaAccountId{
			Value: auth.CloseAuthority,
		},
	}

	_, stat, err := s.sc.SubmitTransaction(txn, solanautil.CommitmentFromProto(req.Commitment))
	if err != nil {
		createAccountFailures.WithLabelValues("unhandled").Inc()
		log.WithError(err).Warn("failed to submit create transaction")
		return nil, status.Errorf(codes.Internal, "unhandled error from SubmitTransaction: %v", err)
	}
	if stat.ErrorResult != nil {
		if solanautil.IsAccountAlreadyExistsError(stat.ErrorResult) {
			createAccountResultCounterVec.WithLabelValues("exists").Inc()

			// Attempt to load the existing data, but if it fails, it's
			// not a _huge_ deal to just return a zero balance, which is
			// the expected case here.
			existing, err := s.infoCache.Get(ctx, info.AccountId.Value)
			if err == nil {
				return &accountpb.CreateAccountResponse{
					Result:      accountpb.CreateAccountResponse_OK,
					AccountInfo: existing,
				}, nil
			} else {
				log.WithError(err).Warn("failed to load existing account info")
			}

			return &accountpb.CreateAccountResponse{
				Result:      accountpb.CreateAccountResponse_OK,
				AccountInfo: info,
			}, nil
		}
		if stat.ErrorResult.ErrorKey() == solana.TransactionErrorBlockhashNotFound {
			createAccountResultCounterVec.WithLabelValues("bad_nonce").Inc()
			return &accountpb.CreateAccountResponse{
				Result: accountpb.CreateAccountResponse_BAD_NONCE,
			}, nil
		} else if stat.ErrorResult.ErrorKey() == solana.TransactionErrorDuplicateSignature {
			createAccountResultCounterVec.WithLabelValues("duplicate_signature").Inc()
		} else {
			createAccountFailures.WithLabelValues("unhandled_stat").Inc()
			log.WithError(stat.ErrorResult).Warn("unexpected transaction error")
		}

		return nil, status.Errorf(codes.Internal, "unhandled error from SubmitTransaction: %v", stat.ErrorResult)
	}

	if err := s.loader.Update(ctx, auth.Owner, info); err != nil {
		log.WithError(err).Warn("failed to update info loader")
	}

	createAccountResultCounterVec.WithLabelValues("ok").Inc()
	return &accountpb.CreateAccountResponse{
		Result:      accountpb.CreateAccountResponse_OK,
		AccountInfo: info,
	}, nil
}

func (s *server) GetAccountInfo(ctx context.Context, req *accountpb.GetAccountInfoRequest) (*accountpb.GetAccountInfoResponse, error) {
	log := s.log.WithField("method", "GetAccountInfo")

	commitment := solanautil.CommitmentFromProto(req.Commitment)

	var stellarFallback bool
	err := s.migrator.InitiateMigration(ctx, req.AccountId.Value, false, commitment)
	switch err {
	case nil, migration.ErrNotFound, migration.ErrBurned:
	default:
		log.WithError(err).Warn("failed to initiate migration")
		stellarFallback = true
	}

	ai, err := s.loader.Load(ctx, req.AccountId.Value, solanautil.CommitmentFromProto(req.Commitment))
	if err == info.ErrAccountInfoNotFound {
		if stellarFallback {
			log.Debug("Looking up horizon balance")
			_, balance, err := s.migrationLoader.LoadAccount(req.AccountId.Value)
			if err == migration.ErrNotFound {
				return &accountpb.GetAccountInfoResponse{
					Result: accountpb.GetAccountInfoResponse_NOT_FOUND,
				}, nil
			} else if err != nil {
				return nil, status.Error(codes.Internal, "failed to get account balance")
			}

			return &accountpb.GetAccountInfoResponse{
				AccountInfo: &accountpb.AccountInfo{
					AccountId: &commonpb.SolanaAccountId{Value: req.AccountId.Value},
					Balance:   int64(balance),
				},
			}, nil
		}

		return &accountpb.GetAccountInfoResponse{
			Result: accountpb.GetAccountInfoResponse_NOT_FOUND,
		}, nil
	} else if err != nil {
		return nil, status.Error(codes.Internal, "failed to retrieve account")
	}

	return &accountpb.GetAccountInfoResponse{
		Result:      accountpb.GetAccountInfoResponse_OK,
		AccountInfo: ai,
	}, nil
}

func (s *server) ResolveTokenAccounts(ctx context.Context, req *accountpb.ResolveTokenAccountsRequest) (*accountpb.ResolveTokenAccountsResponse, error) {
	log := s.log.WithField("method", "ResolveTokenAccounts")

	var cacheWriteback bool
	var accounts []ed25519.PublicKey

	// If we have a cached entry, return it.
	cached, err := s.tokenAccountCache.Get(ctx, req.AccountId.Value)
	if err != nil && err != tokenaccount.ErrTokenAccountsNotFound {
		log.WithError(err).Warn("failed to get token accounts from cache")
	}
	if len(cached) == 0 || rand.Float64() < s.conf.resolveConsistencyCheckRate.Get(ctx) {
		accounts, err = s.sc.GetTokenAccountsByOwner(req.AccountId.Value, s.token)
		if err != nil {
			log.WithError(err).Warn("failed to get token accounts")
			return nil, status.Error(codes.Internal, err.Error())
		}

		if len(cached) != 0 {
			// Only need to writeback if the consistency check has failed
			cacheWriteback = !checkCacheConsistency(cached, accounts)
			resolveAccountHitCounter.Inc()
		} else {
			// Fresh load, always needs a writeback
			cacheWriteback = true
			resolveAccountMissCounter.Inc()
		}
	} else {
		resolveAccountHitCounter.Inc()
		accounts = cached
	}

	// We check to see if the requested account has any accounts that should be
	// migrated.
	migrationAccounts, err := s.migrator.GetMigrationAccounts(ctx, req.AccountId.Value)
	if err == nil && len(migrationAccounts) > 0 {
		// We only use recent here to ensure ResolveAccounts() is still fairly quick.
		//
		// Note: it's important we use the non-mapped value here.
		if err := s.migrator.InitiateMigration(ctx, req.AccountId.Value, false, solana.CommitmentRecent); err != nil && err != migration.ErrNotFound {
			return nil, status.Errorf(codes.Internal, "failed to initiate migration: %v", err)
		}
	}

	resp := &accountpb.ResolveTokenAccountsResponse{
		TokenAccounts:     make([]*commonpb.SolanaAccountId, len(accounts)),
		TokenAccountInfos: make([]*accountpb.AccountInfo, len(accounts)),
	}

	associatedAccount, err := token.GetAssociatedAccount(req.AccountId.Value, s.token)
	if err != nil {
		log.WithError(err).Warn("failed to compute associated account")
		return nil, status.Error(codes.Internal, "failed to compute associated account")
	}

	// The priority of account order is:
	//
	//   1. Associated account
	//   2. Identity account
	//   3. Others (in sort lexicographic order)
	//
	sort.Sort(sortableAccounts(accounts))
	moveFront(accounts, req.AccountId.Value)
	moveFront(accounts, associatedAccount)

	for i := range accounts {
		resp.TokenAccounts[i] = &commonpb.SolanaAccountId{
			Value: accounts[i],
		}

		// todo(perf): could be parallelized
		if req.IncludeAccountInfo {
			ai, err := s.loader.Load(ctx, accounts[i], solana.CommitmentRecent)
			if err == info.ErrAccountInfoNotFound {
				// This implies that the account index and state data are out of sync,
				// which is possible if the two RPC nodes aren't in at the same point
				// and two RPCs occur close together.
				//
				// For now, we just return an internal to cause the client to retry,
				// which may help alleviate the problem. If we see a lot of these, we
				// should look for a better, more involved, solution.
				log.WithError(err).Warn("account index and state are out of sync")
				return nil, status.Errorf(codes.Internal, "account index and state are out of sync")
			} else if err != nil {
				return nil, status.Error(codes.Internal, "failed to load account info")
			}

			resp.TokenAccountInfos[i] = ai
		} else {
			resp.TokenAccountInfos[i] = &accountpb.AccountInfo{
				AccountId: &commonpb.SolanaAccountId{
					Value: accounts[i],
				},
			}
		}
	}

	if cacheWriteback {
		putCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if len(accounts) > 0 {
			if err = s.tokenAccountCache.Put(putCtx, req.AccountId.Value, accounts); err != nil {
				log.WithError(err).Warn("failed to cache token accounts")
			}
		} else {
			if err = s.tokenAccountCache.Delete(putCtx, req.AccountId.Value); err != nil {
				log.WithError(err).Warn("failed to cache token accounts")
			}
		}
	}

	return resp, nil
}

func (s *server) GetEvents(req *accountpb.GetEventsRequest, stream accountpb.Account_GetEventsServer) error {
	log := s.log.WithField("method", "GetEvents")

	tokenAccountID := req.AccountId.Value
	ai, err := s.loader.Load(stream.Context(), req.AccountId.Value, solana.CommitmentRecent)
	if err != nil && err != info.ErrAccountInfoNotFound {
		log.WithError(err).Warn("failed to load account info")
		return status.Error(codes.Internal, err.Error())
	}

	// Currently, mobile SDKs don't resolve owner accounts to token accounts when GetEvents is called on an owner
	// account (resulting in NOT_FOUND). To address this for mobile SDK users without requiring a mobile SDK update,
	// in the cases where the account is not found, we attempt to see if a Kin token account exists for the requested
	// account. If one and only one token account exists, we return events for the resolved token account instead of
	// returning NOT_FOUND.
	if err == info.ErrAccountInfoNotFound {
		accounts, err := s.tokenAccountCache.Get(stream.Context(), req.AccountId.Value)
		if err != nil {
			if err != tokenaccount.ErrTokenAccountsNotFound {
				log.WithError(err).Warn("failed to get token accounts from cache")
			}

			accounts, err = s.sc.GetTokenAccountsByOwner(req.AccountId.Value, s.token)
			if err != nil {
				log.WithError(err).Warn("failed to get token accounts")
				return status.Error(codes.Internal, err.Error())
			}
		}

		if len(accounts) == 0 || len(accounts) > 1 {
			if err := stream.Send(&accountpb.Events{Result: accountpb.Events_NOT_FOUND}); err != nil {
				return status.Error(codes.Internal, err.Error())
			}
			return nil
		}

		tokenAccountID = accounts[0]
		ai, err = s.loader.Load(stream.Context(), tokenAccountID, solana.CommitmentRecent)
		if err != nil && err != info.ErrAccountInfoNotFound {
			log.WithError(err).Warn("failed to load account info")
			return status.Error(codes.Internal, err.Error())
		}

		if err == info.ErrAccountInfoNotFound {
			if err := stream.Send(&accountpb.Events{Result: accountpb.Events_NOT_FOUND}); err != nil {
				return status.Error(codes.Internal, err.Error())
			}
			return nil
		}
	}

	log = log.WithField("account", base58.Encode(req.AccountId.Value)).WithField("token_account", base58.Encode(tokenAccountID))

	// Overwrite AccountId with the requested account ID
	ai.AccountId = &commonpb.SolanaAccountId{Value: req.AccountId.Value}
	err = stream.Send(&accountpb.Events{
		Events: []*accountpb.Event{
			{
				Type: &accountpb.Event_AccountUpdateEvent{
					AccountUpdateEvent: &accountpb.AccountUpdateEvent{
						AccountInfo: ai,
					},
				},
			},
		},
	})
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	accountKey, err := strkey.Encode(strkey.VersionByteAccountID, tokenAccountID)
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

			ai, err := s.loader.Load(stream.Context(), tokenAccountID, solana.CommitmentRecent)
			if err == info.ErrAccountInfoNotFound {
				if err := stream.Send(&accountpb.Events{Result: accountpb.Events_NOT_FOUND}); err != nil {
					return status.Error(codes.Internal, err.Error())
				}
				return nil
			} else if err != nil {
				return status.Error(codes.Internal, err.Error())
			}
			// Overwrite AccountId with the requested account ID
			ai.AccountId = &commonpb.SolanaAccountId{Value: req.AccountId.Value}
			events = append(events, &accountpb.Event{
				Type: &accountpb.Event_AccountUpdateEvent{
					AccountUpdateEvent: &accountpb.AccountUpdateEvent{
						AccountInfo: ai,
					},
				},
			})

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

func (s *server) isWhitelisted(ctx context.Context) (userAgent string, whitelisted bool) {
	if len(s.createWhitelistSecret) == 0 {
		return "", true
	}

	val, err := headers.GetASCIIHeaderByName(ctx, userAgentHeader)
	if err != nil {
		return val, false
	}

	if val == s.createWhitelistSecret {
		return val, true
	}

	if val == "" || !strings.Contains(val, "KinSDK") {
		return val, false
	}

	if strings.Contains(val, "CID/") {
		createAccountByPlatform.WithLabelValues("mobile").Inc()
	} else {
		createAccountByPlatform.WithLabelValues("other").Inc()
	}

	return val, true
}

// checkCacheConsistency returns whether or not the cached set is equivalent to the
// actual set.
func checkCacheConsistency(cached []ed25519.PublicKey, fetched []ed25519.PublicKey) bool {
	if len(cached) != len(fetched) {
		consistencyCheckFailedCounter.Inc()
		return false
	}

	cachedKeys := make(map[string]struct{})
	for _, a := range cached {
		cachedKeys[string(a)] = struct{}{}
	}

	// Note: we don't have to compute the reverse set as we assume that both
	// cached and fetched are distinct. If this is _not_ true, then it is possible
	// for this method to return a false positive.
	for _, a := range fetched {
		if _, ok := cachedKeys[string(a)]; !ok {
			consistencyCheckFailedCounter.Inc()
			return false
		}
	}

	return true
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
	if err := prometheus.Register(nonMigratableResolveCounter); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			nonMigratableResolveCounter = e.ExistingCollector.(prometheus.Counter)
		} else {
			return errors.Wrap(err, "failed to register resolve non migratable resolve counter")
		}
	}
	if err := prometheus.Register(resolveShortCuts); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			resolveShortCuts = e.ExistingCollector.(*prometheus.CounterVec)
		} else {
			return errors.Wrap(err, "failed to register resolve token short cuts counter")
		}
	}

	if err := prometheus.Register(createAccountFailures); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			createAccountFailures = e.ExistingCollector.(*prometheus.CounterVec)
		} else {
			return errors.Wrap(err, "failed to register create account failures")
		}
	}
	if err := prometheus.Register(createAccountByPlatform); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			createAccountByPlatform = e.ExistingCollector.(*prometheus.CounterVec)
		} else {
			return errors.Wrap(err, "failed to register create account by platform")
		}
	}
	if err := prometheus.Register(createAccountBlockCounterVec); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			createAccountBlockCounterVec = e.ExistingCollector.(*prometheus.CounterVec)
		} else {
			return errors.Wrap(err, "failed to register create account block counter vec")
		}
	}
	if err := prometheus.Register(createAccountResultCounterVec); err != nil {
		if e, ok := err.(prometheus.AlreadyRegisteredError); ok {
			createAccountResultCounterVec = e.ExistingCollector.(*prometheus.CounterVec)
		} else {
			return errors.Wrap(err, "failed to register create account result counter vec")
		}
	}

	return nil
}

func moveFront(accounts []ed25519.PublicKey, target ed25519.PublicKey) {
	// The size of accounts is likely to be small, so this is fairly quick.
	//
	// If we find that the target address is in the list, then we place it at the start,
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
	targetIndex := -1
	for i := range accounts {
		if bytes.Equal(accounts[i], target) {
			targetIndex = i
			//offset = 1
			break
		}
	}

	if targetIndex <= 0 {
		return
	}
	for i := targetIndex; i > 0; i-- {
		accounts[i] = accounts[i-1]
	}
	accounts[0] = target
}
