package solana

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
	"github.com/kinecosystem/agora-common/solana/system"
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
	"github.com/kinecosystem/agora/pkg/account/solana/accountinfo"
	"github.com/kinecosystem/agora/pkg/account/solana/tokenaccount"
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
	limiter           *account.Limiter
	accountNotifier   *AccountNotifier
	tokenAccountCache tokenaccount.Cache
	infoCache         accountinfo.Cache
	loader            *accountinfo.Loader
	migrationLoader   migration.Loader
	migrator          migration.Migrator
	mapper            account.Mapper

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
	limiter *account.Limiter,
	accountNotifier *AccountNotifier,
	tokenAccountCache tokenaccount.Cache,
	infoCache accountinfo.Cache,
	loader *accountinfo.Loader,
	migrationLoader migration.Loader,
	migrator migration.Migrator,
	mapper account.Mapper,
	mint ed25519.PublicKey,
	subsidizer ed25519.PrivateKey,
	createWhitelistSecret string,
) (accountpb.AccountServer, error) {
	s := &server{
		log:                   logrus.StandardLogger().WithField("type", "account/solana"),
		sc:                    sc,
		tc:                    token.NewClient(sc, mint),
		accountNotifier:       accountNotifier,
		tokenAccountCache:     tokenAccountCache,
		infoCache:             infoCache,
		loader:                loader,
		migrationLoader:       migrationLoader,
		migrator:              migrator,
		mapper:                mapper,
		limiter:               limiter,
		token:                 mint,
		subsidizer:            subsidizer,
		createWhitelistSecret: createWhitelistSecret,
	}

	conf(&s.conf)

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
			createAccountFailures.WithLabelValues("co_sign").Inc()
			return nil, status.Error(codes.Internal, "failed to co-sign txn")
		}
	}

	info := &accountpb.AccountInfo{
		AccountId: &commonpb.SolanaAccountId{
			Value: sysCreate.Address,
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

	if err := s.mapper.Add(ctx, tokenInitialize.Account, tokenInitialize.Owner); err != nil {
		log.WithError(err).Warn("failed to store account mapping")
		createAccountFailures.WithLabelValues("mapper").Inc()
		return nil, status.Error(codes.Internal, "failed to store account mapping")
	}
	if err := s.loader.Update(ctx, tokenInitialize.Owner, info); err != nil {
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

	info, err := s.loader.Load(ctx, req.AccountId.Value, solanautil.CommitmentFromProto(req.Commitment))
	if err == accountinfo.ErrAccountInfoNotFound {
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
		AccountInfo: info,
	}, nil
}

func (s *server) ResolveTokenAccounts(ctx context.Context, req *accountpb.ResolveTokenAccountsRequest) (*accountpb.ResolveTokenAccountsResponse, error) {
	log := s.log.WithField("method", "ResolveTokenAccounts")

	// Problem:
	//   GetTokensByOwner is _super_ expensive. It does a table scan.
	//   Therefore, we need to avoid this call at all costs.
	//
	// Assumptions:
	//   1. SetAuthority is low to non-existent.
	//     - That is, ownership rarely changes.
	//   2. Key collision is unlikely, and protected by CreateAccount. If we resolve
	//      an incorrect account; it must exist for the kin to be sent to the wrong person.
	//   3. If the account was migrated, we know the mapping happened, and can optimistically return the mapping.
	//   4. If the account was _not_ migrated, then either:
	//     a) it has yet to be migrated.
	//     b) it is a non-migratable kin account.
	//     c) it does not exist.
	//   5. Almost all users of this RPC is from consistent agora users.
	//
	// (1) allows us to cache heavily, so we will do so. If we can invalidate this by observing side
	// operations, we can hopefully keep this.
	//
	// (2) consider the cases that we only return (without verification) the following:
	//    a) Identity
	//    b) Migrated
	//    c) None
	// - If we send to identity, then either the identity was a temporary keypair, or it is a correct
	//   account. To prevent the former, we load GetAccountInfo() to verify.
	// - If we return a migrated account, it must mean that the requested account _was_ a migrated account.
	//   Therefore, this is safe, at least as long as, at least as long as (1) holds true.
	// - None is trivially not a problem in terms of loosing the kin.
	//
	// (3) we can just return the mapping. We know a migration occurred if an entry exists in the migration table.
	//
	// (4a) we can check horizon to know whether or not it _would_ be migrated.
	// (4b) we know from the account-mapper-table, since we write on creation. Note, this requires (5) until
	//      we have a side updater.
	// (4c) to know whether or not it does not exist, we have to assume:
	//   1) a migration did _not_ occur.
	//   2) a native create did not occur.
	//
	// Since we don't have total history for (4c2), we should not make any assumption.
	//
	// Therefore, until we have caught up history, we should:
	//
	// 1. If the account has been migrated, return the map.
	// 2. If the account is not known to be migrated (i.e. not complete):
	//    a) If the account exists in Horizon: GetAccount() on the _derived_ account. Worst case they've sent
	//       to an address that doesn't exist. Note this is protected by the fact that only we can determine
	//       the migration account.
	//    b) else, fall back to the "query the chain" path.
	//
	// Note: we want to do this _after_ the cache, since the cache will (should?) contain values that
	//       have been verified by the chain (at some point).
	var putRequired bool
	var accounts []ed25519.PublicKey

	// If we have a cached entry, return it.
	cached, err := s.tokenAccountCache.Get(ctx, req.AccountId.Value)
	if err != nil && err != tokenaccount.ErrTokenAccountsNotFound {
		log.WithError(err).Warn("failed to get token accounts from cache")
	}

	var shouldMigrate bool
	if len(cached) == 0 {
		resolveAccountMissCounter.Inc()

		if s.conf.resolveShortcutsEnabled.Get(ctx) {
			// Get the set of migration accounts (accounts that _are_ eligible for migration)
			migratedAccounts, err := s.migrator.GetMigrationAccounts(ctx, req.AccountId.Value)
			if err != nil && err != migration.ErrBurned && err != migration.ErrNotFound {
				// Not great, but we can fall back
				log.WithError(err).Warn("failed to check migration status")
			} else if len(migratedAccounts) > 0 {
				// (1) and (2a) from above have the same resultant behavior, so we
				// combine the two.
				shouldMigrate = true
				resolveShortCuts.WithLabelValues("migration").Inc()
				accounts = migratedAccounts
			}
		}
	} else {
		resolveAccountHitCounter.Inc()
		accounts = cached

		// This helps with recovery of migrations. Since the GetStatus table
		// is well scaled, we can do this safely.
		shouldMigrate = true
	}

	if shouldMigrate {
		// We only use recent here to ensure ResolveAccounts() is still fairly quick.
		//
		// Note: it's important we use the non-mapped value here.
		if err := s.migrator.InitiateMigration(ctx, req.AccountId.Value, false, solana.CommitmentRecent); err != nil && err != migration.ErrNotFound {
			return nil, status.Errorf(codes.Internal, "failed to initiate migration: %v", err)
		}
	}

	if len(accounts) == 0 || rand.Float64() < s.conf.resolveConsistencyCheckRate.Get(ctx) {
		putRequired = true
		accounts, err = s.sc.GetTokenAccountsByOwner(req.AccountId.Value, s.token)
		if err != nil {
			log.WithError(err).Warn("failed to get token accounts")
			return nil, status.Error(codes.Internal, err.Error())
		}

		if len(cached) != 0 {
			checkCacheConsistency(cached, accounts)
		}
	}

	resp := &accountpb.ResolveTokenAccountsResponse{
		TokenAccounts: make([]*commonpb.SolanaAccountId, len(accounts)),
	}

	// We sort the set of accounts to make the resolve order deterministic.
	// With the exception of the identity account (outlined below), we just
	// sort them in bytewise order.
	sort.Sort(sortableAccounts(accounts))

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
		putCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if len(resp.TokenAccounts) > 0 {
			keys := make([]ed25519.PublicKey, len(resp.TokenAccounts))
			for i, tokenAccount := range resp.TokenAccounts {
				keys[i] = tokenAccount.Value
				if err = s.mapper.Add(putCtx, keys[i], req.AccountId.Value); err != nil {
					log.WithError(err).Warn("failed to add account mapping")
				}
			}
			if err = s.tokenAccountCache.Put(putCtx, req.AccountId.Value, keys); err != nil {
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
	info, err := s.loader.Load(stream.Context(), req.AccountId.Value, solana.CommitmentRecent)
	if err != nil && err != accountinfo.ErrAccountInfoNotFound {
		log.WithError(err).Warn("failed to load account info")
		return status.Error(codes.Internal, err.Error())
	}

	// Currently, mobile SDKs don't resolve owner accounts to token accounts when GetEvents is called on an owner
	// account (resulting in NOT_FOUND). To address this for mobile SDK users without requiring a mobile SDK update,
	// in the cases where the account is not found, we attempt to see if a Kin token account exists for the requested
	// account. If one and only one token account exists, we return events for the resolved token account instead of
	// returning NOT_FOUND.
	if err == accountinfo.ErrAccountInfoNotFound {
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
		info, err = s.loader.Load(stream.Context(), tokenAccountID, solana.CommitmentRecent)
		if err != nil && err != accountinfo.ErrAccountInfoNotFound {
			log.WithError(err).Warn("failed to load account info")
			return status.Error(codes.Internal, err.Error())
		}

		if err == accountinfo.ErrAccountInfoNotFound {
			if err := stream.Send(&accountpb.Events{Result: accountpb.Events_NOT_FOUND}); err != nil {
				return status.Error(codes.Internal, err.Error())
			}
			return nil
		}
	}

	log = log.WithField("account", base58.Encode(req.AccountId.Value)).WithField("token_account", base58.Encode(tokenAccountID))

	// Overwrite AccountId with the requested account ID
	info.AccountId = &commonpb.SolanaAccountId{Value: req.AccountId.Value}
	err = stream.Send(&accountpb.Events{
		Events: []*accountpb.Event{
			{
				Type: &accountpb.Event_AccountUpdateEvent{
					AccountUpdateEvent: &accountpb.AccountUpdateEvent{
						AccountInfo: info,
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

			info, err := s.loader.Load(stream.Context(), tokenAccountID, solana.CommitmentRecent)
			if err == accountinfo.ErrAccountInfoNotFound {
				if err := stream.Send(&accountpb.Events{Result: accountpb.Events_NOT_FOUND}); err != nil {
					return status.Error(codes.Internal, err.Error())
				}
				return nil
			} else if err != nil {
				return status.Error(codes.Internal, err.Error())
			}
			// Overwrite AccountId with the requested account ID
			info.AccountId = &commonpb.SolanaAccountId{Value: req.AccountId.Value}
			events = append(events, &accountpb.Event{
				Type: &accountpb.Event_AccountUpdateEvent{
					AccountUpdateEvent: &accountpb.AccountUpdateEvent{
						AccountInfo: info,
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
