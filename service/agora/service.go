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
	"github.com/aws/aws-sdk-go/aws/session"
	dynamodbv1 "github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/go-redis/redis/v7"
	"github.com/go-redis/redis_rate/v8"
	agoraapp "github.com/kinecosystem/agora-common/app"
	"github.com/kinecosystem/agora-common/headers"
	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/agora/pkg/webhook"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v3"
	transactionpb "github.com/kinecosystem/agora-api/genproto/transaction/v3"

	accountserver "github.com/kinecosystem/agora/pkg/account/server"
	appconfigdb "github.com/kinecosystem/agora/pkg/app/dynamodb"
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

	// Configurable keystore options:
	_ "github.com/kinecosystem/agora/pkg/keypair/dynamodb"
	_ "github.com/kinecosystem/agora/pkg/keypair/environment"
	_ "github.com/kinecosystem/agora/pkg/keypair/memory"
)

const (
	rootKeypairIDEnv      = "ROOT_KEYPAIR_ID"
	whitelistKeypairIDEnv = "WHITELIST_KEYPAIR_ID"
	keystoreTypeEnv       = "KEYSTORE_TYPE"

	redisConnStringEnv = "REDIS_CONN_STRING"

	// Rate Limit Configs
	createAccountGlobalRLEnv = "CREATE_ACCOUNT_GLOBAL_LIMIT"
	submitTxGlobalRLEnv      = "SUBMIT_TX_GLOBAL_LIMIT"
	submitTxAppRLEnv         = "SUBMIT_TX_APP_LIMIT"
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

	redisConnString := os.Getenv(redisConnStringEnv)
	if redisConnString == "" {
		return errors.Errorf("%s must be set", redisConnStringEnv)
	}

	hosts := strings.Split(redisConnString, ",")
	addrs := make(map[string]string)
	for i, host := range hosts {
		addrs[fmt.Sprintf("server%d", i)] = host
	}

	limiter := redis_rate.NewLimiter(redis.NewRing(&redis.RingOptions{
		Addrs: addrs,
	}))

	accountNotifier := accountserver.NewAccountNotifier(client)

	dynamoClient := dynamodb.New(cfg)
	appConfigStore := appconfigdb.New(dynamoClient)
	invoiceStore := invoicedb.New(dynamoClient)
	webhookClient := webhook.NewClient(&http.Client{Timeout: 10 * time.Second})

	if err != nil {
		return errors.Wrap(err, "failed to create rate limiter")
	}

	createAccountRLStr := os.Getenv(createAccountGlobalRLEnv)
	createAccLimit, err := strconv.Atoi(createAccountRLStr)
	if err != nil {
		return errors.Wrapf(err, "%s must be an integer", createAccountGlobalRLEnv)
	}

	submitTxGlobalRLStr := os.Getenv(submitTxGlobalRLEnv)
	submitTxGlobalLimit, err := strconv.Atoi(submitTxGlobalRLStr)
	if err != nil {
		return errors.Wrapf(err, "%s must be an integer", submitTxGlobalRLEnv)
	}

	submitTxAppRLStr := os.Getenv(submitTxAppRLEnv)
	submitTxAppLimit, err := strconv.Atoi(submitTxAppRLStr)
	if err != nil {
		return errors.Wrapf(err, "%s must be an integer", submitTxAppRLEnv)
	}

	a.accountServer = accountserver.New(
		rootAccountKP,
		client,
		accountNotifier,
		limiter,
		&accountserver.Config{CreateAccountGlobalLimit: createAccLimit},
	)

	a.txnServer, err = transactionserver.New(
		whitelistAccountKP,
		appConfigStore,
		invoiceStore,
		client,
		clientV2,
		webhookClient,
		limiter,
		&transactionserver.Config{
			SubmitTxGlobalLimit: submitTxGlobalLimit,
			SubmitTxAppLimit:    submitTxAppLimit,
		},
	)
	if err != nil {
		return errors.Wrap(err, "failed to init transaction server")
	}

	ctx, cancel := context.WithCancel(context.Background())
	a.streamCancelFunc = cancel

	committer := ingestioncommitter.New(dynamoClient)
	rw := historyrw.New(dynamoClient)
	ingestor := stellar.New(model.KinVersion_KIN3, clientV2)
	lock, err := ingestionlock.New(dynamodbv1.New(sess), "kin3_ingestion", 10*time.Second)
	if err != nil {
		return errors.Wrap(err, "failed to init ingestion lock")
	}

	go func() {
		transaction.StreamTransactions(ctx, clientV2, accountNotifier)
	}()
	go func() {
		err := ingestion.Run(ctx, lock, committer, rw, ingestor)
		if err != nil && err != context.Canceled {
			log.WithError(err).Warn("ingestion loop terminated")
		} else {
			log.WithError(err).Info("ingestion loop terminated")
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
