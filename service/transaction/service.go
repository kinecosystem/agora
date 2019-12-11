package main

import (
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	agoraapp "github.com/kinecosystem/agora-common/app"
	"github.com/kinecosystem/agora-common/env"
	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/agora-transaction-services/pkg/appindex/static"
	agoradata "github.com/kinecosystem/agora-transaction-services/pkg/data/dynamodb"
	"github.com/kinecosystem/agora-transaction-services/pkg/transaction/server"
	"github.com/kinecosystem/kin-api/genproto/transaction/v3"
)

type app struct {
	txnServer transaction.TransactionServer

	shutdown   sync.Once
	shutdownCh chan struct{}
}

// Init implements agorapp.App.Init.
func (a *app) Init(_ agoraapp.AppConfig) error {
	a.shutdownCh = make(chan struct{})

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
		return err
	}

	e, err := env.FromEnvVariable()
	if err != nil {
		return errors.Wrapf(err, "failed to get env")
	}

	store, err := agoradata.New(e, dynamodb.New(cfg))
	if err != nil {
		return errors.Wrapf(err, "failed to initialize agoradata store")
	}

	a.txnServer = server.New(
		store,
		static.New(),
		client,
		clientV2,
	)

	return nil
}

// RegisterWithGRPC implements agorapp.App.RegisterWithGRPC.
func (a *app) RegisterWithGRPC(server *grpc.Server) {
	transaction.RegisterTransactionServer(server, a.txnServer)
}

// ShutdownChan implements agorapp.App.ShutdownChan.
func (a *app) ShutdownChan() <-chan struct{} {
	return a.shutdownCh
}

// Stop implements agorapp.App.Stop.
func (a *app) Stop() {
	a.shutdown.Do(func() {
		close(a.shutdownCh)
	})
}

func main() {
	if err := agoraapp.Run(&app{}); err != nil {
		logrus.WithError(err).Fatal("error running service")
	}
}
