package main

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"net"
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
	"github.com/kinecosystem/agora-common/kin"
	kinversion "github.com/kinecosystem/agora-common/kin/version"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	sqstasks "github.com/kinecosystem/agora-common/taskqueue/sqs"
	"github.com/kinecosystem/go/clients/horizon"
	"github.com/kinecosystem/go/strkey"
	"github.com/mr-tron/base58"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"

	accountpbv4 "github.com/kinecosystem/agora-api/genproto/account/v4"
	airdroppb "github.com/kinecosystem/agora-api/genproto/airdrop/v4"
	transactionpbv4 "github.com/kinecosystem/agora-api/genproto/transaction/v4"

	"github.com/kinecosystem/agora/pkg/account/info"
	infodb "github.com/kinecosystem/agora/pkg/account/info/dynamodb"
	accountserver "github.com/kinecosystem/agora/pkg/account/server"
	"github.com/kinecosystem/agora/pkg/account/tokenaccount"
	accountcache "github.com/kinecosystem/agora/pkg/account/tokenaccount/dynamodb"
	airdropserver "github.com/kinecosystem/agora/pkg/airdrop/server"
	appconfigdb "github.com/kinecosystem/agora/pkg/app/dynamodb"
	appmapper "github.com/kinecosystem/agora/pkg/app/dynamodb/mapper"
	redisevents "github.com/kinecosystem/agora/pkg/events/redis"
	invoicedb "github.com/kinecosystem/agora/pkg/invoice/dynamodb"
	"github.com/kinecosystem/agora/pkg/keypair"
	keypairdb "github.com/kinecosystem/agora/pkg/keypair"
	"github.com/kinecosystem/agora/pkg/migration"
	migrationstore "github.com/kinecosystem/agora/pkg/migration/dynamodb"
	stellarmigrator "github.com/kinecosystem/agora/pkg/migration/stellar"
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

	etcdEndpointsEnv = "ETCD_ENDPOINTS"

	// Solana config
	solanaEndpointEnv      = "SOLANA_ENDPOINT"
	kinTokenEnv            = "KIN_TOKEN"
	airdropSourceEnv       = "AIRDROP_SOURCE"
	subsidizerKeypairIDEnv = "SUBSIDIZER_KEYPAIR_ID"

	// Solana kin2 migration config
	migratorHorizon2URLEnv = "MIGRATOR_HORIZON2_CLIENT_URL"
	kin2SourceEnv          = "KIN2_SOURCE_ADDRESS"
	kin2SourceKeyEnv       = "KIN2_SOURCE_KEYPAIR_ID"
	kin2MigrationSecretEnv = "KIN2_MIGRATION_SECRET"

	// Solana kin3 migration config
	migratorHorizon3URLEnv   = "MIGRATOR_HORIZON3_CLIENT_URL"
	mintEnv                  = "MINT_ADDRESS"
	mintKeyEnv               = "MINT_KEYPAIR_ID"
	kin3MigrationSecretEnv   = "KIN3_MIGRATION_SECRET"
	createWhitelistSecretEnv = "CREATE_WHITELIST_SECRET"

	// Rate Limit Configs
	createAccountGlobalRLEnv = "CREATE_ACCOUNT_GLOBAL_LIMIT"
	submitTxGlobalRLEnv      = "SUBMIT_TX_GLOBAL_LIMIT"
	submitTxAppRLEnv         = "SUBMIT_TX_APP_LIMIT"
	rlRedisConnStringEnv     = "RL_REDIS_CONN_STRING"
	migrationKin2LimitEnv    = "MIGRATION_KIN2_LIMIT"
	migrationKin3LimitEnv    = "MIGRATION_KIN3_LIMIT"

	// Events config
	eventsRedisConnStringEnv = "EVENTS_REDIS_CONN_STRING"

	// Token Account Cache Configs
	tokenAccountTTLEnv = "TOKEN_ACCOUNT_TTL"

	accountInfoTTL         = 30 * time.Second
	negativeAccountInfoTTL = 15 * time.Second
	dedupeTTL              = 24 * time.Hour
)

var (
	// We use the same transport for both horizon and Solana, as the total
	// number of system connections is often what causes problems.
	transport = &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     false,
		MaxIdleConns:          64,
		MaxConnsPerHost:       512,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
)

type app struct {
	etcdClient *clientv3.Client

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

	kin2Client, err := kin.GetKin2Client()
	if err != nil {
		return errors.Wrap(err, "failed to get kin2 client")
	}
	kin3Client, err := kin.GetClient()
	if err != nil {
		return errors.Wrap(err, "failed to get kin3 client")
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

	createAccountRL, err := parseRateLimit(createAccountGlobalRLEnv)
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
	migrationKin2RL, err := parseRateLimit(migrationKin2LimitEnv)
	if err != nil {
		return err
	}
	migrationKin3RL, err := parseRateLimit(migrationKin3LimitEnv)
	if err != nil {
		return err
	}

	var limiter *redis_rate.Limiter
	if createAccountRL > 0 || submitTxGlobalRL > 0 || submitTxAppRL > 0 || migrationKin2RL > 0 || migrationKin3RL > 0 {
		rlRedisConnString := os.Getenv(rlRedisConnStringEnv)
		if rlRedisConnString == "" {
			return errors.Errorf("%s must be set", rlRedisConnStringEnv)
		}
		limiter = redis_rate.NewLimiter(redis.NewRing(&redis.RingOptions{
			Addrs: parseAddrsFromConnString(rlRedisConnString),
		}))
	}

	accountLimiter := accountserver.NewLimiter(rate.NewRedisRateLimiter(limiter, redis_rate.PerSecond(createAccountRL)))

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
	authorizer, err := transaction.NewAuthorizer(
		appMapper,
		appConfigStore,
		webhookClient,
		txLimiter,
	)
	if err != nil {
		return errors.Wrap(err, "failed to initialize authorizer")
	}

	solanaClient := solana.New(os.Getenv(solanaEndpointEnv))

	kinToken, err := base58.Decode(os.Getenv(kinTokenEnv))
	if err != nil {
		return errors.Wrap(err, "failed to parse kin token address")
	}
	tokenAccountTTLSeconds, err := strconv.Atoi(os.Getenv(tokenAccountTTLEnv))
	if err != nil {
		return errors.Wrap(err, "failed to parse token account TTL")
	}

	var subsidizer []byte
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
	var airdropSource []byte
	if os.Getenv(airdropSourceEnv) != "" {
		airdropSource, err = base58.Decode(os.Getenv(airdropSourceEnv))
		if err != nil {
			log.Fatal(err)
		}
	}

	if len(subsidizer) > 0 && len(airdropSource) > 0 {
		a.airdropServer = airdropserver.New(solanaClient, kinToken, airdropSource, subsidizer, subsidizer)
	}

	issuer, err := kin.GetKin2Issuer()
	if err != nil {
		return errors.Wrap(err, "failed to get kin2 issuer")
	}

	var migrator2Client *horizon.Client
	if os.Getenv(migratorHorizon2URLEnv) != "" {
		migrator2Client = &horizon.Client{
			URL: os.Getenv(migratorHorizon2URLEnv),
			HTTP: &http.Client{
				Transport: transport,
				Timeout:   30 * time.Second,
			},
		}
	} else {
		migrator2Client = kin2Client
	}

	var migrator3Client *horizon.Client
	if os.Getenv(migratorHorizon3URLEnv) != "" {
		migrator3Client = &horizon.Client{
			URL: os.Getenv(migratorHorizon3URLEnv),
			HTTP: &http.Client{
				Transport: transport,
				Timeout:   30 * time.Second,
			},
		}
	} else {
		migrator3Client = kin3Client
	}

	kin2Loader := stellarmigrator.NewKin2Loader(migrator2Client, issuer)
	kin3Loader := stellarmigrator.NewKin3Loader(migrator3Client)
	compositeLoader := stellarmigrator.NewCompositeLoader(kin2Loader, kin3Loader)

	kin2Migrator, err := initializeKin2Migrator(
		migrationstore.New(dynamoClient, kinversion.KinVersion2),
		keystore,
		rate.NewRedisRateLimiter(limiter, redis_rate.PerSecond(migrationKin2RL)),
		solanaClient,
		kin2Loader,
		kinToken,
		subsidizer,
	)
	if err != nil {
		return errors.Wrap(err, "failed to initialize kin2 migrator")
	}
	kin3Migrator, err := initializeKin3Migrator(
		migrationstore.New(dynamoClient, kinversion.KinVersion3),
		keystore,
		rate.NewRedisRateLimiter(limiter, redis_rate.PerSecond(migrationKin3RL)),
		solanaClient,
		kin3Loader,
		kinToken,
		subsidizer,
	)
	if err != nil {
		return errors.Wrap(err, "failed to initialize kin2 migrator")
	}

	migrationConf := migration.WithEnvConfig()
	if a.etcdClient != nil {
		migrationConf = migration.WithETCDConfigs(a.etcdClient)
	}
	migrator := migration.NewDualChainMigrator(kin2Migrator, kin3Migrator, migrationConf)

	kin4AccountNotifier := accountserver.NewAccountNotifier()
	tokenAccountCache := accountcache.New(dynamoClient, time.Duration(tokenAccountTTLSeconds)*time.Second)
	cacheInvalidator, err := tokenaccount.NewCacheUpdater(tokenAccountCache, kinToken)
	if err != nil {
		return errors.Wrap(err, "faild to initialize token account cache invalidator")
	}
	tokenClient := token.NewClient(solanaClient, kinToken)
	infoCache := infodb.NewCache(dynamoClient, accountInfoTTL, negativeAccountInfoTTL)
	deduper := deduper.New(dynamoClient, dedupeTTL)

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

	accountConfig := accountserver.WithEnvConfig()
	if a.etcdClient != nil {
		accountConfig = accountserver.WithETCDConfigs(a.etcdClient)
	}
	a.accountSolana, err = accountserver.New(
		accountConfig,
		solanaClient,
		accountLimiter,
		kin4AccountNotifier,
		tokenAccountCache,
		infoCache,
		info.NewLoader(tokenClient, infoCache, tokenAccountCache),
		compositeLoader,
		migrator,
		kinToken,
		subsidizer,
		createWhitelistSecret,
	)
	if err != nil {
		return errors.Wrap(err, "failed to initialize v4 account serve")
	}

	a.txnSolana = txnserver.New(
		solanaClient,
		invoiceStore,
		historyRW,
		committer,
		authorizer,
		compositeLoader,
		migrator,
		infoCache,
		eventsProcessor,
		redisEvents,
		deduper,
		kinToken,
		subsidizer,
	)

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

func initializeKin2Migrator(
	store migration.Store,
	keystore keypair.Keystore,
	limiter rate.Limiter,
	sc solana.Client,
	loader migration.Loader,
	kinToken ed25519.PublicKey,
	subsidizer ed25519.PrivateKey,
) (migration.Migrator, error) {
	if os.Getenv(kin2SourceEnv) == "" {
		return nil, errors.New("must specify kin2 source")
	}
	if os.Getenv(kin2SourceKeyEnv) == "" {
		return nil, errors.New("must specify kin2 source key")
	}
	if os.Getenv(kin2MigrationSecretEnv) == "" {
		return nil, errors.New("must specify kin2 migration secret")
	}

	kin2Source, err := base58.Decode(os.Getenv(kin2SourceEnv))
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse kin2 source")
	}
	kin2SourceKP, err := keystore.Get(context.Background(), os.Getenv(kin2SourceKeyEnv))
	if err != nil {
		return nil, errors.Wrap(err, "failed to determine kin2 source key")
	}
	rawSeed, err := strkey.Decode(strkey.VersionByteSeed, kin2SourceKP.Seed())
	if err != nil {
		return nil, errors.Wrap(err, "invalid kin2 source seed string")
	}
	kin2SourceKey := ed25519.NewKeyFromSeed(rawSeed)

	secret, err := agoraapp.LoadFile(os.Getenv(kin2MigrationSecretEnv))
	if err != nil {
		return nil, errors.Wrap(err, "failed to get kin2 migration secret")
	}
	if strings.Contains(string(secret), "\n") {
		return nil, errors.Wrap(err, "secret contains a newline")
	}

	return stellarmigrator.New(
		store,
		kinversion.KinVersion2,
		sc,
		loader,
		limiter,
		kinToken,
		subsidizer,
		kin2Source,
		kin2SourceKey,
		secret,
	), nil
}

func initializeKin3Migrator(
	store migration.Store,
	keystore keypair.Keystore,
	limiter rate.Limiter,
	sc solana.Client,
	loader migration.Loader,
	kinToken ed25519.PublicKey,
	subsidizer ed25519.PrivateKey,
) (migration.Migrator, error) {
	if os.Getenv(mintEnv) == "" {
		return nil, errors.New("must specify mint")
	}
	if os.Getenv(mintKeyEnv) == "" {
		return nil, errors.New("must specify mint key")
	}
	if os.Getenv(kin3MigrationSecretEnv) == "" {
		return nil, errors.New("must specify kin3 migration secret")
	}

	mint, err := base58.Decode(os.Getenv(mintEnv))
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse mint")
	}
	mintKP, err := keystore.Get(context.Background(), os.Getenv(mintKeyEnv))
	if err != nil {
		return nil, errors.Wrap(err, "failed to determine mint key")
	}
	rawSeed, err := strkey.Decode(strkey.VersionByteSeed, mintKP.Seed())
	if err != nil {
		return nil, errors.Wrap(err, "invalid mint seed string")
	}
	mintKey := ed25519.NewKeyFromSeed(rawSeed)

	secret, err := agoraapp.LoadFile(os.Getenv(kin3MigrationSecretEnv))
	if err != nil {
		return nil, errors.Wrap(err, "failed to get kin3 migration secret")
	}
	if strings.Contains(string(secret), "\n") {
		return nil, errors.Wrap(err, "secret contains a newline")
	}

	return stellarmigrator.New(
		store,
		kinversion.KinVersion3,
		sc,
		loader,
		limiter,
		kinToken,
		subsidizer,
		mint,
		mintKey,
		secret,
	), nil
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
	versionConf := version.WithEnvConfig()

	var err error
	var etcdClient *clientv3.Client
	if os.Getenv(etcdEndpointsEnv) != "" {
		etcdClient, err = clientv3.New(clientv3.Config{
			Endpoints:            strings.Split(os.Getenv(etcdEndpointsEnv), ","),
			DialTimeout:          5 * time.Second,
			DialKeepAliveTime:    10 * time.Second,
			DialKeepAliveTimeout: 10 * time.Second,
		})
		if err != nil {
			log.WithError(err).Fatal("error running service")
		}

		versionConf = version.WithETCDConfig(etcdClient)
	}

	if err := agoraapp.Run(
		&app{etcdClient: etcdClient},
		agoraapp.WithUnaryServerInterceptor(headers.UnaryServerInterceptor()),
		agoraapp.WithUnaryServerInterceptor(version.DisabledVersionUnaryServerInterceptor(versionConf)),
		agoraapp.WithStreamServerInterceptor(headers.StreamServerInterceptor()),
		agoraapp.WithStreamServerInterceptor(version.DisabledVersionStreamServerInterceptor(versionConf)),
		agoraapp.WithHTTPGatewayEnabled(true, httpgateway.WithCORSEnabled(true)),
	); err != nil {
		log.WithError(err).Fatal("error running service")
	}
}
