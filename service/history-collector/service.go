package main

import (
	"context"
	"net/url"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go/aws/session"
	dynamodbv1 "github.com/aws/aws-sdk-go/service/dynamodb"
	agoraapp "github.com/kinecosystem/agora-common/app"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	infodb "github.com/kinecosystem/agora/pkg/account/solana/accountinfo/dynamodb"
	"github.com/mr-tron/base58"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/option"
	"google.golang.org/grpc"

	historyreader "github.com/kinecosystem/agora/pkg/transaction/history/dynamodb"
	ingestioncommitter "github.com/kinecosystem/agora/pkg/transaction/history/ingestion/dynamodb/committer"
	ingestionlock "github.com/kinecosystem/agora/pkg/transaction/history/ingestion/dynamodb/locker"
	"github.com/kinecosystem/agora/pkg/transaction/history/kre"
	bqsubmitter "github.com/kinecosystem/agora/pkg/transaction/history/kre/bigquery"
)

const (
	solanaEndpointEnv = "SOLANA_ENDPOINT"
	kinTokenEnv       = "KIN_TOKEN"

	bqCredentialsEnv    = "BQ_CREDENTIALS"
	bqCreationsTableEnv = "BQ_CREATIONS_TABLE"
	bqPaymentsTableEnv  = "BQ_PAYMENTS_TABLE"
)

type app struct {
	shutdown         sync.Once
	loaderCancelFunc context.CancelFunc
	shutdownCh       chan struct{}
}

// Init implements agorapp.App.Init.
func (a *app) Init(_ agoraapp.Config) error {
	a.shutdownCh = make(chan struct{})

	if os.Getenv(solanaEndpointEnv) == "" {
		return errors.New("missing solana endpoint")
	}

	solanaClient := solana.New(os.Getenv(solanaEndpointEnv))

	if os.Getenv(kinTokenEnv) == "" {
		return errors.New("missing kin token")
	}

	kinToken, err := base58.Decode(os.Getenv(kinTokenEnv))
	if err != nil {
		return errors.Wrap(err, "failed to parse kin token")
	}

	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		return errors.Wrap(err, "failed to init v2 aws sdk")
	}
	dynamoClient := dynamodb.New(cfg)
	hist := historyreader.New(dynamoClient)
	committer := ingestioncommitter.New(dynamoClient)
	accountStore := infodb.NewStore(dynamoClient)

	sess, err := session.NewSession()
	if err != nil {
		return errors.Wrap(err, "failed to init v1 aws sdk")
	}
	historyLock, err := ingestionlock.New(dynamodbv1.New(sess), "ingestor_kre", 10*time.Second)
	if err != nil {
		return errors.Wrap(err, "failed to create history locker")
	}

	if os.Getenv(bqCreationsTableEnv) == "" {
		return errors.Errorf("missing %s", bqCreationsTableEnv)
	}
	if os.Getenv(bqPaymentsTableEnv) == "" {
		return errors.Errorf("missing %s", bqPaymentsTableEnv)
	}

	bqCredentials := os.Getenv(bqCredentialsEnv)
	if bqCredentials == "" {
		return errors.Errorf("missing %s", bqCredentialsEnv)
	}
	authOption := option.WithAPIKey(bqCredentials)
	if _, err := url.Parse(bqCredentials); err == nil {
		creds, err := agoraapp.LoadFile(bqCredentials)
		if err == nil {
			authOption = option.WithCredentialsJSON(creds)
		}
	}
	bqClient, err := bigquery.NewClient(
		context.Background(),
		"kin-bi",
		authOption,
	)
	if err != nil {
		return errors.Wrap(err, "failed to intiialize bigquery client")
	}

	ctx, cancel := context.WithCancel(context.Background())
	a.loaderCancelFunc = cancel

	loader := kre.NewLoader(
		hist,
		committer,
		historyLock,
		solanaClient,
		token.NewClient(solanaClient, kinToken),
		bqsubmitter.New(bqClient, os.Getenv(bqCreationsTableEnv)),
		bqsubmitter.New(bqClient, os.Getenv(bqPaymentsTableEnv)),
		accountStore,
	)
	go func() {
		err := loader.Process(ctx, 5*time.Minute)
		if err != nil && err != context.Canceled {
			log.WithError(err).Warn("loader loop terminated")
		} else {
			log.WithError(err).Info("loader loop terminated")
		}
	}()

	return nil
}

// RegisterWithGRPC implements agorapp.App.RegisterWithGRPC.
func (a *app) RegisterWithGRPC(server *grpc.Server) {
}

// ShutdownChan implements agorapp.App.ShutdownChan.
func (a *app) ShutdownChan() <-chan struct{} {
	return a.shutdownCh
}

// Stop implements agorapp.App.Stop.
func (a *app) Stop() {
	a.shutdown.Do(func() {
		close(a.shutdownCh)

		a.loaderCancelFunc()
	})
}

func main() {
	if err := agoraapp.Run(&app{}); err != nil {
		log.WithError(err).Fatal("error running service")
	}
}
