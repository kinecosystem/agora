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
	"github.com/aws/aws-sdk-go/aws/session"
	dynamodbv1 "github.com/aws/aws-sdk-go/service/dynamodb"
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
	"google.golang.org/grpc"

	accountpbv3 "github.com/kinecosystem/agora-api/genproto/account/v3"
	accountpbv4 "github.com/kinecosystem/agora-api/genproto/account/v4"
	airdroppb "github.com/kinecosystem/agora-api/genproto/airdrop/v4"
	transactionpbv3 "github.com/kinecosystem/agora-api/genproto/transaction/v3"
	transactionpbv4 "github.com/kinecosystem/agora-api/genproto/transaction/v4"

	"github.com/kinecosystem/agora/pkg/account"
	mapperdb "github.com/kinecosystem/agora/pkg/account/dynamodb"
	accountsolana "github.com/kinecosystem/agora/pkg/account/solana"
	infodb "github.com/kinecosystem/agora/pkg/account/solana/accountinfo/dynamodb"
	"github.com/kinecosystem/agora/pkg/account/solana/tokenaccount"
	accountcache "github.com/kinecosystem/agora/pkg/account/solana/tokenaccount/dynamodb"
	accountstellar "github.com/kinecosystem/agora/pkg/account/stellar"
	airdropserver "github.com/kinecosystem/agora/pkg/airdrop/server"
	appconfigdb "github.com/kinecosystem/agora/pkg/app/dynamodb"
	appmapper "github.com/kinecosystem/agora/pkg/app/dynamodb/mapper"
	"github.com/kinecosystem/agora/pkg/channel"
	channelpool "github.com/kinecosystem/agora/pkg/channel/dynamodb"
	redisevents "github.com/kinecosystem/agora/pkg/events/redis"
	invoicedb "github.com/kinecosystem/agora/pkg/invoice/dynamodb"
	keypairdb "github.com/kinecosystem/agora/pkg/keypair"
	"github.com/kinecosystem/agora/pkg/migration"
	migrationstore "github.com/kinecosystem/agora/pkg/migration/dynamodb"
	kin3migrator "github.com/kinecosystem/agora/pkg/migration/kin3"
	"github.com/kinecosystem/agora/pkg/rate"
	"github.com/kinecosystem/agora/pkg/transaction"
	deduper "github.com/kinecosystem/agora/pkg/transaction/dedupe/dynamodb"
	historyrw "github.com/kinecosystem/agora/pkg/transaction/history/dynamodb"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
	ingestioncommitter "github.com/kinecosystem/agora/pkg/transaction/history/ingestion/dynamodb/committer"
	ingestionlock "github.com/kinecosystem/agora/pkg/transaction/history/ingestion/dynamodb/locker"
	stellaringestor "github.com/kinecosystem/agora/pkg/transaction/history/ingestion/stellar"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
	transactionsolana "github.com/kinecosystem/agora/pkg/transaction/solana"
	transactionstellar "github.com/kinecosystem/agora/pkg/transaction/stellar"
	"github.com/kinecosystem/agora/pkg/version"
	"github.com/kinecosystem/agora/pkg/webhook"
	webevents "github.com/kinecosystem/agora/pkg/webhook/events"

	// Configurable keystore options:
	_ "github.com/kinecosystem/agora/pkg/keypair/dynamodb"
	_ "github.com/kinecosystem/agora/pkg/keypair/environment"
	_ "github.com/kinecosystem/agora/pkg/keypair/memory"
)

const (
	rootKeypairIDEnv     = "ROOT_KEYPAIR_ID"
	rootKin2KeypairIDEnv = "ROOT_KIN_2_KEYPAIR_ID"
	keystoreTypeEnv      = "KEYSTORE_TYPE"

	// Solana config
	solanaEndpointEnv         = "SOLANA_ENDPOINT"
	solanaResolveEndpointEnv  = "SOLANA_RESOLVE_ENDPOINT"
	solanaSubmitEndpointEnv   = "SOLANA_SUBMIT_ENDPOINT"
	solanaMigratorEndpointEnv = "SOLANA_MIGRATOR_ENDPOINT"
	kinTokenEnv               = "KIN_TOKEN"
	airdropSourceEnv          = "AIRDROP_SOURCE"
	subsidizerKeypairIDEnv    = "SUBSIDIZER_KEYPAIR_ID"

	// Solana kin2 migration config
	//kin2SourceAddressEnv   = "KIN2_SOURCE_ADDRESS"
	//kin2SourceKeyEnv       = "KIN2_SOURCE_KEYPAIR_ID"
	//kin2MigrationSecretEnv = "KIN2_MIGRATION_SECRET"

	// Solana kin3 migration config
	migratorHorizonURLEnv    = "MIGRATOR_HORIZON_CLIENT_URL"
	mintEnv                  = "MINT_ADDRESS"
	mintKeyEnv               = "MINT_KEYPAIR_ID"
	kin3MigrationSecretEnv   = "KIN3_MIGRATION_SECRET"
	createWhitelistSecretEnv = "CREATE_WHITELIST_SECRET"

	// Rate Limit Configs
	createAccountGlobalRLEnv = "CREATE_ACCOUNT_GLOBAL_LIMIT"
	submitTxGlobalRLEnv      = "SUBMIT_TX_GLOBAL_LIMIT"
	submitTxAppRLEnv         = "SUBMIT_TX_APP_LIMIT"
	rlRedisConnStringEnv     = "RL_REDIS_CONN_STRING"
	migrationGlobalLimitEnv  = "MIGRATION_GLOBAL_LIMIT"

	// Events config
	eventsRedisConnStringEnv = "EVENTS_REDIS_CONN_STRING"

	// Channel Configs
	maxChannelsEnv = "MAX_CHANNELS"
	channelSaltEnv = "CHANNEL_SALT"

	// Token Account Cache Configs
	tokenAccountTTLEnv      = "TOKEN_ACCOUNT_TTL"
	consistencyCheckProbEnv = "TOKEN_ACCOUNT_CONSISTENCY_CHECK_PROBABILITY"

	accountInfoTTL         = 30 * time.Second
	negativeAccountInfoTTL = 15 * time.Second
	dedupeTTL              = 24 * time.Hour
)

type app struct {
	accountStellar accountpbv3.AccountServer
	accountSolana  accountpbv4.AccountServer
	txnStellar     transactionpbv3.TransactionServer
	txnSolana      transactionpbv4.TransactionServer
	airdropServer  airdroppb.AirdropServer

	streamCancelFunc context.CancelFunc

	shutdown   sync.Once
	shutdownCh chan struct{}
}

// Init implements agorapp.App.Init.
func (a *app) Init(_ agoraapp.Config) error {
	a.shutdownCh = make(chan struct{})

	keystoreType := os.Getenv(keystoreTypeEnv)
	keystore, err := keypairdb.CreateStore(keystoreType)
	if err != nil {
		return errors.Wrapf(err, "failed to create keystore using configured : %s", keystoreType)
	}

	rootAccountKP, err := keystore.Get(context.Background(), os.Getenv(rootKeypairIDEnv))
	if err != nil {
		return errors.Wrap(err, "failed to determine root keypair")
	}

	kin2RootAccountKP, err := keystore.Get(context.Background(), os.Getenv(rootKin2KeypairIDEnv))
	if err != nil {
		return errors.Wrap(err, "failed to determine kin 2 root keypair")
	}

	client, err := kin.GetClient()
	if err != nil {
		return errors.Wrap(err, "failed to get kin client")
	}
	kin2Client, err := kin.GetKin2Client()
	if err != nil {
		return errors.Wrap(err, "failed to get kin 2 client")
	}

	clientV2, err := kin.GetClientV2()
	if err != nil {
		return errors.Wrap(err, "failed to get v2 kin client")
	}
	kin2ClientV2, err := kin.GetKin2ClientV2()
	if err != nil {
		return errors.Wrap(err, "failed to get v2 kin 2 client")
	}

	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		return errors.Wrap(err, "failed to init v2 aws sdk")
	}

	// dynamodb locking library requires aws v1, unfortunately.
	sess, err := session.NewSession()
	if err != nil {
		return errors.Wrap(err, "failed to init v1 aws sdk")
	}
	dynamoV1Client := dynamodbv1.New(sess)

	accountNotifier := accountstellar.NewAccountNotifier()
	kin2AccountNotifier := accountstellar.NewAccountNotifier()

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

	migrationGlobalRL, err := parseRateLimit(migrationGlobalLimitEnv)
	if err != nil {
		return err
	}

	var limiter *redis_rate.Limiter
	if createAccountRL > 0 || submitTxGlobalRL > 0 || submitTxAppRL > 0 || migrationGlobalRL > 0 {
		rlRedisConnString := os.Getenv(rlRedisConnStringEnv)
		if rlRedisConnString == "" {
			return errors.Errorf("%s must be set", rlRedisConnStringEnv)
		}
		limiter = redis_rate.NewLimiter(redis.NewRing(&redis.RingOptions{
			Addrs: parseAddrsFromConnString(rlRedisConnString),
		}))
	}

	var channelPool channel.Pool
	var kin2ChannelPool channel.Pool

	maxChannelsStr := os.Getenv(maxChannelsEnv)
	if maxChannelsStr != "" && maxChannelsStr != "0" {
		maxChannels, err := strconv.Atoi(maxChannelsStr)
		if err != nil || maxChannels < 0 {
			return errors.Errorf("%s must be set to an integer >= 0", maxChannelsEnv)
		}

		channelSalt := os.Getenv(channelSaltEnv)
		if channelSalt == "" {
			return errors.Errorf("%s must be set if %s is set", channelSaltEnv, maxChannelsEnv)
		}

		channelPool, err = channelpool.New(
			dynamoV1Client,
			maxChannels,
			kinversion.KinVersion3,
			rootAccountKP,
			channelSalt,
		)
		if err != nil {
			return errors.Wrap(err, "failed to initialize channel pool")
		}

		kin2ChannelPool, err = channelpool.New(
			dynamoV1Client,
			maxChannels,
			kinversion.KinVersion2,
			kin2RootAccountKP,
			channelSalt,
		)
		if err != nil {
			return errors.Wrap(err, "failed to initialize channel pool")
		}
	}

	accountLimiter := account.NewLimiter(rate.NewRedisRateLimiter(limiter, redis_rate.PerSecond(createAccountRL)))
	a.accountStellar, err = accountstellar.New(
		rootAccountKP,
		client,
		accountNotifier,
		channelPool,
		kin2RootAccountKP,
		kin2Client,
		kin2AccountNotifier,
		kin2ChannelPool,
		accountLimiter,
	)
	if err != nil {
		return errors.Wrap(err, "failed to init account server")
	}

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

	network, err := kin.GetNetwork()
	if err != nil {
		return errors.Wrap(err, "failed to get kin network")
	}
	kin2Network, err := kin.GetKin2Network()
	if err != nil {
		return errors.Wrap(err, "failed to get kin 2 network")
	}

	committer := ingestioncommitter.New(dynamoClient)
	historyIngestor := stellaringestor.New(ingestion.GetHistoryIngestorName(model.KinVersion_KIN3), model.KinVersion_KIN3, clientV2, network.Passphrase)
	eventsIngestor := stellaringestor.New(ingestion.GetEventsIngestorName(model.KinVersion_KIN3), model.KinVersion_KIN3, clientV2, network.Passphrase)
	historyLock, err := ingestionlock.New(dynamodbv1.New(sess), "ingestor_history_kin3", 10*time.Second)
	if err != nil {
		return errors.Wrap(err, "failed to init history ingestion lock")
	}
	eventsLock, err := ingestionlock.New(dynamodbv1.New(sess), "ingestor_events_kin3", 10*time.Second)
	if err != nil {
		return errors.Wrap(err, "failed to init events ingestion lock")
	}

	kin2HistoryIngestor := stellaringestor.New(ingestion.GetHistoryIngestorName(model.KinVersion_KIN2), model.KinVersion_KIN2, kin2ClientV2, kin2Network.Passphrase)
	kin2EventsIngestor := stellaringestor.New(ingestion.GetEventsIngestorName(model.KinVersion_KIN2), model.KinVersion_KIN2, kin2ClientV2, kin2Network.Passphrase)
	kin2HistoryLock, err := ingestionlock.New(dynamodbv1.New(sess), "ingestor_history_kin2", 10*time.Second)
	if err != nil {
		return errors.Wrap(err, "failed to init kin 2 history ingestion lock")
	}
	kin2EventsLock, err := ingestionlock.New(dynamodbv1.New(sess), "ingestor_events_kin2", 10*time.Second)
	if err != nil {
		return errors.Wrap(err, "failed to init kin 2 events ingestion lock")
	}

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

	a.txnStellar, err = transactionstellar.New(
		appConfigStore,
		appMapper,
		invoiceStore,
		historyRW,
		committer,
		authorizer,
		client,
		kin2Client,
	)
	if err != nil {
		return errors.Wrap(err, "failed to init transaction server")
	}

	//
	// Kin3 Ingestion and Streams
	//
	go func() {
		transactionstellar.StreamTransactions(ctx, clientV2, accountNotifier)
	}()
	go func() {
		err := ingestion.Run(ctx, historyLock, committer, historyRW, historyIngestor)
		if err != nil && err != context.Canceled {
			log.WithError(err).Warn("history ingestion loop terminated")
		} else {
			log.WithError(err).Info("history ingestion loop terminated")
		}
	}()
	go func() {
		err := ingestion.Run(ctx, eventsLock, committer, eventsProcessor, eventsIngestor)
		if err != nil && err != context.Canceled {
			log.WithError(err).Warn("events ingestion loop terminated")
		} else {
			log.WithError(err).Info("events ingestion loop terminated")
		}
	}()
	//
	// Kin2 Ingestion and Streams
	//
	go func() {
		transactionstellar.StreamTransactions(ctx, kin2ClientV2, kin2AccountNotifier)
	}()
	go func() {
		err := ingestion.Run(ctx, kin2HistoryLock, committer, historyRW, kin2HistoryIngestor)
		if err != nil && err != context.Canceled {
			log.WithError(err).Warn("kin 2 history ingestion loop terminated")
		} else {
			log.WithError(err).Info("kin 2 history ingestion loop terminated")
		}
	}()
	go func() {
		err := ingestion.Run(ctx, kin2EventsLock, committer, eventsProcessor, kin2EventsIngestor)
		if err != nil && err != context.Canceled {
			log.WithError(err).Warn("kin 2 events ingestion loop terminated")
		} else {
			log.WithError(err).Info("kin 2 events ingestion loop terminated")
		}
	}()

	if os.Getenv(solanaEndpointEnv) != "" {
		var (
			solanaClient         solana.Client
			solanaResolveClient  solana.Client
			solanaSubmitClient   solana.Client
			solanaMigratorClient solana.Client
		)

		solanaClient = solana.New(os.Getenv(solanaEndpointEnv))

		if os.Getenv(solanaResolveEndpointEnv) != "" {
			solanaResolveClient = solana.New(os.Getenv(solanaResolveEndpointEnv))
		} else {
			solanaResolveClient = solanaClient
		}

		if os.Getenv(solanaSubmitEndpointEnv) != "" {
			solanaSubmitClient = solana.New(os.Getenv(solanaSubmitEndpointEnv))
		} else {
			solanaSubmitClient = solanaClient
		}

		if os.Getenv(solanaMigratorEndpointEnv) != "" {
			solanaMigratorClient = solana.New(os.Getenv(solanaMigratorEndpointEnv))
		} else {
			solanaMigratorClient = solanaClient
		}

		kinToken, err := base58.Decode(os.Getenv(kinTokenEnv))
		if err != nil {
			return errors.Wrap(err, "failed to parse kin token address")
		}

		tokenAccountTTLSeconds, err := strconv.Atoi(os.Getenv(tokenAccountTTLEnv))
		if err != nil {
			return errors.Wrap(err, "failed to parse token account TTL")
		}

		consistencyCheckFreq, err := strconv.ParseFloat(os.Getenv(consistencyCheckProbEnv), 32)
		if err != nil {
			return errors.Wrap(err, "failed to parse token account consistency check frequency (should be in range [0.0, 1.0])")
		} else if consistencyCheckFreq > 1.0 {
			return errors.Errorf("invalid token account consistency check frequency (should be in range [0.0, 1.0]): %f", consistencyCheckFreq)
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

		kin3MigrationSecret, err := agoraapp.LoadFile(os.Getenv(kin3MigrationSecretEnv))
		if err != nil {
			return errors.Wrap(err, "failed to get migration secret")
		}
		if strings.Contains(string(kin3MigrationSecret), "\n") {
			return errors.New("secret contains a newline")
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

		var mint ed25519.PublicKey
		if os.Getenv(mintEnv) != "" {
			mint, err = base58.Decode(os.Getenv(mintEnv))
			if err != nil {
				return errors.Wrap(err, "failed to parse mint")
			}
		}
		var mintKey ed25519.PrivateKey
		if os.Getenv(mintKeyEnv) != "" {
			mintKP, err := keystore.Get(context.Background(), os.Getenv(mintKeyEnv))
			if err != nil {
				return errors.Wrap(err, "failed to determine mint keypair")
			}

			rawSeed, err := strkey.Decode(strkey.VersionByteSeed, mintKP.Seed())
			if err != nil {
				return errors.Wrap(err, "invalid mint seed string")
			}

			mintKey = ed25519.NewKeyFromSeed(rawSeed)
		}

		var kin3Migrator migration.Migrator
		migrationStore := migrationstore.New(dynamoClient)

		var migratorHorizonClient *horizon.Client
		if os.Getenv(migratorHorizonURLEnv) != "" {
			migratorHorizonClient = &horizon.Client{
				URL:  os.Getenv(migratorHorizonURLEnv),
				HTTP: http.DefaultClient,
			}
		} else {
			migratorHorizonClient = client
		}

		if len(kin3MigrationSecret) > 0 {
			if len(mint) == 0 || len(mintKey) == 0 {
				return errors.New("must specify mint and mint key if kin3 migration is enabled")
			}

			kin3Migrator = kin3migrator.New(
				migrationStore,
				solanaMigratorClient,
				migratorHorizonClient,
				rate.NewRedisRateLimiter(limiter, redis_rate.PerSecond(migrationGlobalRL)),
				kinToken,
				subsidizer,
				mint,
				mintKey,
				kin3MigrationSecret,
			)
		} else {
			kin3Migrator = migration.NewNoopMigrator()
		}

		kin4AccountNotifier := accountsolana.NewAccountNotifier()
		tokenAccountCache := accountcache.New(dynamoClient, time.Duration(tokenAccountTTLSeconds)*time.Second)
		cacheInvalidator, err := tokenaccount.NewCacheUpdater(tokenAccountCache, kinToken)
		if err != nil {
			return errors.Wrap(err, "faild to initialize token account cache invalidator")
		}
		mapperStore := mapperdb.New(dynamoClient)
		mapper := account.NewMapper(token.NewClient(solanaClient, kinToken), mapperStore)
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
			transactionsolana.MapTransactionEvent(kin4AccountNotifier),
			transactionsolana.MapTransactionEvent(cacheInvalidator),
		)
		if err != nil {
			return errors.Wrap(err, "failed to initialize redis events")
		}

		a.accountSolana, err = accountsolana.New(
			solanaClient,
			solanaResolveClient,
			solanaSubmitClient,
			migratorHorizonClient,
			accountLimiter,
			kin4AccountNotifier,
			tokenAccountCache,
			infoCache,
			kin3Migrator,
			migrationStore,
			mapper,
			kinToken,
			subsidizer,
			float32(consistencyCheckFreq),
			createWhitelistSecret,
		)
		if err != nil {
			return errors.Wrap(err, "failed to initialize v4 account serve")
		}

		a.txnSolana = transactionsolana.New(
			solanaClient,
			solanaSubmitClient,
			invoiceStore,
			historyRW,
			committer,
			authorizer,
			kin3Migrator,
			infoCache,
			eventsProcessor,
			redisEvents,
			deduper,
			kinToken,
			subsidizer,
			migratorHorizonClient,
		)

		//
		// Kin4 Streams
		//
		go func() {
			transactionsolana.StreamTransactions(ctx, solanaClient, kin4AccountNotifier, cacheInvalidator)
		}()
	} else {
		a.txnSolana = transactionsolana.NewNoopServer()
	}

	return nil
}

// RegisterWithGRPC implements agorapp.App.RegisterWithGRPC.
func (a *app) RegisterWithGRPC(server *grpc.Server) {
	accountpbv3.RegisterAccountServer(server, a.accountStellar)
	transactionpbv3.RegisterTransactionServer(server, a.txnStellar)

	if a.airdropServer != nil {
		airdroppb.RegisterAirdropServer(server, a.airdropServer)
	}
	if a.accountSolana != nil {
		accountpbv4.RegisterAccountServer(server, a.accountSolana)
	}

	transactionpbv4.RegisterTransactionServer(server, a.txnSolana)
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

func main() {
	if err := agoraapp.Run(
		&app{},
		agoraapp.WithUnaryServerInterceptor(headers.UnaryServerInterceptor()),
		agoraapp.WithUnaryServerInterceptor(version.DisabledVersionUnaryServerInterceptor(kinversion.KinVersion3, []int{3})),
		agoraapp.WithStreamServerInterceptor(headers.StreamServerInterceptor()),
		agoraapp.WithStreamServerInterceptor(version.DisabledVersionStreamServerInterceptor(kinversion.KinVersion3, []int{3})),
		agoraapp.WithHTTPGatewayEnabled(true, httpgateway.WithCORSEnabled(true)),
	); err != nil {
		log.WithError(err).Fatal("error running service")
	}
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
