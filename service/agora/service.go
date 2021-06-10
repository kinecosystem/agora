package main

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/go-redis/redis/v7"
	"github.com/go-redis/redis_rate/v8"
	agoraapp "github.com/kinecosystem/agora-common/app"
	"github.com/kinecosystem/agora-common/headers"
	"github.com/kinecosystem/agora-common/httpgateway"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	sqstasks "github.com/kinecosystem/agora-common/taskqueue/sqs"
	"github.com/kinecosystem/go/strkey"
	"github.com/mr-tron/base58"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	accountpbv3 "github.com/kinecosystem/agora-api/genproto/account/v3"
	accountpbv4 "github.com/kinecosystem/agora-api/genproto/account/v4"
	airdroppb "github.com/kinecosystem/agora-api/genproto/airdrop/v4"
	transactionpbv3 "github.com/kinecosystem/agora-api/genproto/transaction/v3"
	transactionpbv4 "github.com/kinecosystem/agora-api/genproto/transaction/v4"

	"github.com/kinecosystem/agora/pkg/account"
	"github.com/kinecosystem/agora/pkg/account/info"
	infodb "github.com/kinecosystem/agora/pkg/account/info/dynamodb"
	accountserver "github.com/kinecosystem/agora/pkg/account/server"
	"github.com/kinecosystem/agora/pkg/account/specstate"
	"github.com/kinecosystem/agora/pkg/account/tokenaccount"
	accountcache "github.com/kinecosystem/agora/pkg/account/tokenaccount/dynamodb"
	airdropserver "github.com/kinecosystem/agora/pkg/airdrop/server"
	appconfigdb "github.com/kinecosystem/agora/pkg/app/dynamodb"
	appmapper "github.com/kinecosystem/agora/pkg/app/dynamodb/mapper"
	redisevents "github.com/kinecosystem/agora/pkg/events/redis"
	invoicedb "github.com/kinecosystem/agora/pkg/invoice/dynamodb"
	keypairdb "github.com/kinecosystem/agora/pkg/keypair"
	"github.com/kinecosystem/agora/pkg/rate"
	"github.com/kinecosystem/agora/pkg/solanautil"
	"github.com/kinecosystem/agora/pkg/transaction"
	deduper "github.com/kinecosystem/agora/pkg/transaction/dedupe/dynamodb"
	historyrw "github.com/kinecosystem/agora/pkg/transaction/history/dynamodb"
	ingestioncommitter "github.com/kinecosystem/agora/pkg/transaction/history/ingestion/dynamodb/committer"
	txnserver "github.com/kinecosystem/agora/pkg/transaction/server"
	"github.com/kinecosystem/agora/pkg/version"
	"github.com/kinecosystem/agora/pkg/webhook"
	webevents "github.com/kinecosystem/agora/pkg/webhook/events"

	// Configurable keystore options:
	_ "github.com/kinecosystem/agora/pkg/keypair/dynamodb"
	_ "github.com/kinecosystem/agora/pkg/keypair/environment"
	_ "github.com/kinecosystem/agora/pkg/keypair/memory"
)

const (
	keystoreTypeEnv = "KEYSTORE_TYPE"

	// Solana config
	solanaEndpointEnv        = "SOLANA_ENDPOINT"
	kinTokenEnv              = "KIN_TOKEN"
	airdropSourceEnv         = "AIRDROP_SOURCE"
	subsidizerKeypairIDEnv   = "SUBSIDIZER_KEYPAIR_ID"
	createWhitelistSecretEnv = "CREATE_WHITELIST_SECRET"

	// Rate Limit Configs
	createAccountGlobalRLEnv = "CREATE_ACCOUNT_GLOBAL_LIMIT"
	createAccountAppRLEnv    = "CREATE_ACCOUNT_APP_LIMIT"
	submitTxGlobalRLEnv      = "SUBMIT_TX_GLOBAL_LIMIT"
	submitTxAppRLEnv         = "SUBMIT_TX_APP_LIMIT"
	rlRedisConnStringEnv     = "RL_REDIS_CONN_STRING"

	// Events config
	eventsRedisConnStringEnv = "EVENTS_REDIS_CONN_STRING"

	// Token Account Cache Configs
	tokenAccountTTLEnv = "TOKEN_ACCOUNT_TTL"

	accountInfoTTL         = 30 * time.Second
	negativeAccountInfoTTL = 15 * time.Second
	dedupeTTL              = 24 * time.Hour
)

type app struct {
	accountSolana accountpbv4.AccountServer
	txnSolana     transactionpbv4.TransactionServer
	airdropServer airdroppb.AirdropServer

	streamCancelFunc context.CancelFunc

	shutdown   sync.Once
	shutdownCh chan struct{}
}

// Init implements agorapp.App.Init.
func (a *app) Init(_ agoraapp.Config) (err error) {
	a.shutdownCh = make(chan struct{})

	keystoreType := os.Getenv(keystoreTypeEnv)
	keystore, err := keypairdb.CreateStore(keystoreType)
	if err != nil {
		return errors.Wrapf(err, "failed to create keystore using configured : %s", keystoreType)
	}

	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		return errors.Wrap(err, "failed to init v2 aws sdk")
	}

	dynamoClient := dynamodb.New(cfg)
	appConfigStore := appconfigdb.New(dynamoClient)
	appMapper := appmapper.New(dynamoClient)
	invoiceStore := invoicedb.New(dynamoClient)
	webhookClient := webhook.NewClient(&http.Client{Timeout: 10 * time.Second})

	createAccountGlobalRL, err := parseRateLimit(createAccountGlobalRLEnv)
	if err != nil {
		return err
	}
	createAccountAppRL, err := parseRateLimit(createAccountAppRLEnv)
	if err != nil {
		return err
	}

	submitTxGlobalRL, err := parseRateLimit(submitTxGlobalRLEnv)
	if err != nil {
		return err
	}
	submitTxAppRL, err := parseRateLimit(submitTxAppRLEnv)
	if err != nil {
		return err
	}

	var limiter *redis_rate.Limiter
	if createAccountGlobalRL > 0 || createAccountAppRL > 0 || submitTxGlobalRL > 0 || submitTxAppRL > 0 {
		rlRedisConnString := os.Getenv(rlRedisConnStringEnv)
		if rlRedisConnString == "" {
			return errors.Errorf("%s must be set", rlRedisConnStringEnv)
		}
		limiter = redis_rate.NewLimiter(redis.NewRing(&redis.RingOptions{
			Addrs: parseAddrsFromConnString(rlRedisConnString),
		}))
	}

	accountLimiter := account.NewLimiter(
		func(r int) rate.Limiter {
			return rate.NewRedisRateLimiter(limiter, redis_rate.PerSecond(r))
		},
		createAccountGlobalRL,
		createAccountAppRL,
	)

	ctx, cancel := context.WithCancel(context.Background())
	a.streamCancelFunc = cancel

	eventsProcessor, err := webevents.NewProcessor(
		sqstasks.NewProcessorCtor(
			webevents.IngestionQueueName,
			sqs.New(cfg),
		),
		invoiceStore,
		appConfigStore,
		appMapper,
		webhookClient,
	)
	if err != nil {
		return errors.Wrap(err, "failed to init events processor")
	}

	committer := ingestioncommitter.New(dynamoClient)
	historyRW := historyrw.New(dynamoClient)
	txLimiter := transaction.NewLimiter(
		func(limit int) rate.Limiter {
			return rate.NewRedisRateLimiter(limiter, redis_rate.PerSecond(limit))
		},
		submitTxGlobalRL,
		submitTxAppRL,
	)

	solanaClient := solana.New(os.Getenv(solanaEndpointEnv))

	kinToken, err := base58.Decode(os.Getenv(kinTokenEnv))
	if err != nil {
		return errors.Wrap(err, "failed to parse kin token address")
	}
	tokenAccountTTLSeconds, err := strconv.Atoi(os.Getenv(tokenAccountTTLEnv))
	if err != nil {
		return errors.Wrap(err, "failed to parse token account TTL")
	}

	var subsidizer ed25519.PrivateKey
	if os.Getenv(subsidizerKeypairIDEnv) != "" {
		subsidizerKP, err := keystore.Get(context.Background(), os.Getenv(subsidizerKeypairIDEnv))
		if err != nil {
			return errors.Wrap(err, "failed to determine subsidizer keypair")
		}

		rawSeed, err := strkey.Decode(strkey.VersionByteSeed, subsidizerKP.Seed())
		if err != nil {
			return errors.Wrap(err, "invalid subsidizer seed string")
		}

		subsidizer = ed25519.NewKeyFromSeed(rawSeed)
		solanautil.MonitorAccount(
			ctx,
			solanaClient,
			ed25519.PrivateKey(subsidizer).Public().(ed25519.PublicKey),
		)
	}

	kin4AccountNotifier := accountserver.NewAccountNotifier()
	tokenAccountCache := accountcache.New(dynamoClient, time.Duration(tokenAccountTTLSeconds)*time.Second)
	cacheInvalidator, err := tokenaccount.NewCacheUpdater(tokenAccountCache, kinToken)
	if err != nil {
		return errors.Wrap(err, "faild to initialize token account cache invalidator")
	}
	tokenClient := token.NewClient(solanaClient, kinToken)
	infoCache := infodb.NewCache(dynamoClient, accountInfoTTL, negativeAccountInfoTTL)
	deduper := deduper.New(dynamoClient, dedupeTTL)
	specStateLoader := specstate.NewSpeculativeLoader(tokenClient, infoCache)

	if os.Getenv(eventsRedisConnStringEnv) == "" {
		return errors.New("missing events redis connection string")
	}
	redisEvents, err := redisevents.New(
		redis.NewClient(&redis.Options{
			Addr: os.Getenv(eventsRedisConnStringEnv),
		}),
		redisevents.TransactionChannel,
		txnserver.MapTransactionEvent(kin4AccountNotifier),
		txnserver.MapTransactionEvent(cacheInvalidator),
	)
	if err != nil {
		return errors.Wrap(err, "failed to initialize redis events")
	}

	var createWhitelistSecret string
	if len(os.Getenv(createWhitelistSecretEnv)) > 0 {
		loaded, err := agoraapp.LoadFile(os.Getenv(createWhitelistSecretEnv))
		if err != nil {
			return errors.Wrap(err, "failed to get create whitelist secret")
		}
		if strings.Contains(string(loaded), "\n") {
			return errors.New("secret contains a newline")
		}
		createWhitelistSecret = string(loaded)
	}

	accountLamports, err := solanaClient.GetMinimumBalanceForRentExemption(token.AccountSize)
	if err != nil {
		return errors.Wrap(err, "failed to load minimum balance for rent exception")
	}

	accountConfig := accountserver.WithEnvConfig()
	accountAuthorizer := account.NewAuthorizer(
		appMapper,
		appConfigStore,
		webhookClient,
		accountLimiter,
		accountLamports,
		subsidizer,
		kinToken,
	)
	a.accountSolana = accountserver.New(
		accountConfig,
		solanaClient,
		kin4AccountNotifier,
		tokenAccountCache,
		infoCache,
		info.NewLoader(tokenClient, infoCache, tokenAccountCache),
		accountAuthorizer,
		kinToken,
		subsidizer,
		createWhitelistSecret,
		accountLamports,
	)

	authorizer, err := transaction.NewAuthorizer(
		appMapper,
		appConfigStore,
		webhookClient,
		txLimiter,
		subsidizer,
		kinToken,
		accountLamports,
	)
	if err != nil {
		return errors.Wrap(err, "failed to initialize authorizer")
	}
	a.txnSolana = txnserver.New(
		solanaClient,
		invoiceStore,
		historyRW,
		committer,
		authorizer,
		eventsProcessor,
		redisEvents,
		deduper,
		specStateLoader,
		kinToken,
		subsidizer,
	)

	var airdropSource []byte
	if os.Getenv(airdropSourceEnv) != "" {
		airdropSource, err = base58.Decode(os.Getenv(airdropSourceEnv))
		if err != nil {
			log.Fatal(err)
		}
	}

	if len(subsidizer) > 0 && len(airdropSource) > 0 {
		a.airdropServer = airdropserver.New(solanaClient, kinToken, airdropSource, subsidizer, subsidizer, specStateLoader)
	}

	//
	// Kin4 Streams
	//
	go func() {
		txnserver.StreamTransactions(ctx, solanaClient, kin4AccountNotifier, cacheInvalidator)
	}()

	return nil
}

// RegisterWithGRPC implements agorapp.App.RegisterWithGRPC.
func (a *app) RegisterWithGRPC(server *grpc.Server) {
	accountpbv3.RegisterAccountServer(server, &accountpbv3.UnimplementedAccountServer{})
	transactionpbv3.RegisterTransactionServer(server, &transactionpbv3.UnimplementedTransactionServer{})

	accountpbv4.RegisterAccountServer(server, a.accountSolana)
	transactionpbv4.RegisterTransactionServer(server, a.txnSolana)

	if a.airdropServer != nil {
		airdroppb.RegisterAirdropServer(server, a.airdropServer)
	}
}

// ShutdownChan implements agorapp.App.ShutdownChan.
func (a *app) ShutdownChan() <-chan struct{} {
	return a.shutdownCh
}

// Stop implements agorapp.App.Stop.
func (a *app) Stop() {
	a.shutdown.Do(func() {
		close(a.shutdownCh)
		a.streamCancelFunc()
	})
}

func parseAddrsFromConnString(redisConnString string) map[string]string {
	hosts := strings.Split(redisConnString, ",")
	addrs := make(map[string]string)
	for i, host := range hosts {
		addrs[fmt.Sprintf("server%d", i)] = host
	}
	return addrs
}

func parseRateLimit(env string) (int, error) {
	rlStr := os.Getenv(env)
	if rlStr == "" {
		return -1, nil
	}

	val, err := strconv.Atoi(rlStr)
	if err != nil {
		return -1, errors.Wrapf(err, "if set, %s must be an integer", env)
	}

	if val <= 0 {
		return -1, errors.Errorf("if set, %s must be > 0", env)
	}
	return val, nil
}

func main() {
	if err := agoraapp.Run(
		&app{},
		agoraapp.WithUnaryServerInterceptor(headers.UnaryServerInterceptor()),
		agoraapp.WithUnaryServerInterceptor(version.DisabledVersionUnaryServerInterceptor()),
		agoraapp.WithStreamServerInterceptor(headers.StreamServerInterceptor()),
		agoraapp.WithStreamServerInterceptor(version.DisabledVersionStreamServerInterceptor()),
		agoraapp.WithHTTPGatewayEnabled(true, httpgateway.WithCORSEnabled(true)),
	); err != nil {
		log.WithError(err).Fatal("error running service")
	}
}
