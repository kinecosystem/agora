package main

import (
	"context"
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
	"github.com/kinecosystem/agora-common/kin"
	sqstasks "github.com/kinecosystem/agora-common/taskqueue/sqs"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v3"
	transactionpb "github.com/kinecosystem/agora-api/genproto/transaction/v3"

	accountserver "github.com/kinecosystem/agora/pkg/account/server"
	appconfigdb "github.com/kinecosystem/agora/pkg/app/dynamodb"
	"github.com/kinecosystem/agora/pkg/channel"
	channelpool "github.com/kinecosystem/agora/pkg/channel/dynamodb"
	invoicedb "github.com/kinecosystem/agora/pkg/invoice/dynamodb"
	keypairdb "github.com/kinecosystem/agora/pkg/keypair"
	"github.com/kinecosystem/agora/pkg/transaction"
	historyrw "github.com/kinecosystem/agora/pkg/transaction/history/dynamodb"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
	ingestioncommitter "github.com/kinecosystem/agora/pkg/transaction/history/ingestion/dynamodb/committer"
	ingestionlock "github.com/kinecosystem/agora/pkg/transaction/history/ingestion/dynamodb/locker"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion/stellar"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
	transactionserver "github.com/kinecosystem/agora/pkg/transaction/server"
	"github.com/kinecosystem/agora/pkg/webhook"
	"github.com/kinecosystem/agora/pkg/webhook/events"

	// Configurable keystore options:
	_ "github.com/kinecosystem/agora/pkg/keypair/dynamodb"
	_ "github.com/kinecosystem/agora/pkg/keypair/environment"
	_ "github.com/kinecosystem/agora/pkg/keypair/memory"
)

const (
	rootKeypairIDEnv      = "ROOT_KEYPAIR_ID"
	whitelistKeypairIDEnv = "WHITELIST_KEYPAIR_ID"
	keystoreTypeEnv       = "KEYSTORE_TYPE"

	// Rate Limit Configs
	createAccountGlobalRLEnv = "CREATE_ACCOUNT_GLOBAL_LIMIT"
	submitTxGlobalRLEnv      = "SUBMIT_TX_GLOBAL_LIMIT"
	submitTxAppRLEnv         = "SUBMIT_TX_APP_LIMIT"
	rlRedisConnStringEnv     = "RL_REDIS_CONN_STRING"

	// Channel Configs
	maxChannelsEnv = "MAX_CHANNELS"
	channelSaltEnv = "CHANNEL_SALT"
)

type app struct {
	accountServer accountpb.AccountServer
	txnServer     transactionpb.TransactionServer

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

	whitelistAccountKP, err := keystore.Get(context.Background(), os.Getenv(whitelistKeypairIDEnv))
	if err != nil {
		return errors.Wrap(err, "failed to load whitelist keypair")
	}

	client, err := kin.GetClient()
	if err != nil {
		return errors.Wrap(err, "failed to get kin client")
	}

	clientV2, err := kin.GetClientV2()
	if err != nil {
		return errors.Wrap(err, "failed to get kin client")
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

	accountNotifier := accountserver.NewAccountNotifier(client)

	dynamoClient := dynamodb.New(cfg)
	appConfigStore := appconfigdb.New(dynamoClient)
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

	var limiter *redis_rate.Limiter
	if createAccountRL > 0 || submitTxGlobalRL > 0 || submitTxAppRL > 0 {
		rlRedisConnString := os.Getenv(rlRedisConnStringEnv)
		if rlRedisConnString == "" {
			return errors.Errorf("%s must be set", rlRedisConnStringEnv)
		}

		hosts := strings.Split(rlRedisConnString, ",")
		addrs := make(map[string]string)
		for i, host := range hosts {
			addrs[fmt.Sprintf("server%d", i)] = host
		}

		limiter = redis_rate.NewLimiter(redis.NewRing(&redis.RingOptions{
			Addrs: parseAddrsFromConnString(rlRedisConnString),
		}))
	}

	var channelPool channel.Pool
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

		pool, err := channelpool.New(
			dynamoV1Client,
			maxChannels,
			rootAccountKP,
			channelSalt,
		)
		if err != nil {
			return errors.Wrap(err, "failed to initialize channel pool")
		}

		channelPool = pool
	}

	a.accountServer, err = accountserver.New(
		rootAccountKP,
		client,
		accountNotifier,
		limiter,
		channelPool,
		&accountserver.Config{
			CreateAccountGlobalLimit: createAccountRL,
		},
	)
	if err != nil {
		return errors.Wrap(err, "failed to init account server")
	}

	ctx, cancel := context.WithCancel(context.Background())
	a.streamCancelFunc = cancel

	eventsProcessor, err := events.NewProcessor(
		sqstasks.NewProcessorCtor(
			events.IngestionQueueName,
			sqs.New(cfg),
		),
		invoiceStore,
		appConfigStore,
		webhookClient,
	)
	if err != nil {
		return errors.Wrap(err, "failed to init events processor")
	}

	network, err := kin.GetNetwork()
	if err != nil {
		return errors.Wrap(err, "failed to get kin network")
	}

	committer := ingestioncommitter.New(dynamoClient)
	historyIngestor := stellar.New("history", model.KinVersion_KIN3, clientV2, network.Passphrase)
	eventsIngestor := stellar.New("events", model.KinVersion_KIN3, clientV2, network.Passphrase)
	historyLock, err := ingestionlock.New(dynamodbv1.New(sess), "ingestor_history_kin3", 10*time.Second)
	if err != nil {
		return errors.Wrap(err, "failed to init history ingestion lock")
	}
	eventsLock, err := ingestionlock.New(dynamodbv1.New(sess), "ingestor_events_kin3", 10*time.Second)
	if err != nil {
		return errors.Wrap(err, "failed to init events ingestion lock")
	}

	historyRW := historyrw.New(dynamoClient)

	a.txnServer, err = transactionserver.New(
		whitelistAccountKP,
		appConfigStore,
		invoiceStore,
		historyRW,
		committer,
		client,
		clientV2,
		webhookClient,
		limiter,
		&transactionserver.Config{
			SubmitTxGlobalLimit: submitTxGlobalRL,
			SubmitTxAppLimit:    submitTxAppRL,
		},
	)
	if err != nil {
		return errors.Wrap(err, "failed to init transaction server")
	}

	go func() {
		transaction.StreamTransactions(ctx, clientV2, accountNotifier)
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

	return nil
}

// RegisterWithGRPC implements agorapp.App.RegisterWithGRPC.
func (a *app) RegisterWithGRPC(server *grpc.Server) {
	accountpb.RegisterAccountServer(server, a.accountServer)
	transactionpb.RegisterTransactionServer(server, a.txnServer)
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
		agoraapp.WithStreamServerInterceptor(headers.StreamServerInterceptor()),
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
