package main

import (
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	agoraapp "github.com/kinecosystem/agora-common/app"
	"github.com/kinecosystem/agora-common/kin"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	transactionpb "github.com/kinecosystem/kin-api/genproto/transaction/v3"

	"github.com/kinecosystem/agora-transaction-services-internal/pkg/appindex/static"
	agoradatadb "github.com/kinecosystem/agora-transaction-services-internal/pkg/data/dynamodb"
	"github.com/kinecosystem/agora-transaction-services-internal/pkg/transaction/server"
	invoicedb "github.com/kinecosystem/agora-transaction-services/pkg/invoice/dynamodb"
)

type app struct {
	txnServer transactionpb.TransactionServer

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

	dynamoClient := dynamodb.New(cfg)
	store := agoradatadb.New(dynamoClient)
	invoiceStore := invoicedb.New(dynamoClient)

	a.txnServer = server.New(
		store,
		invoiceStore,
		static.New(),
		client,
		clientV2,
	)

	return nil
}

// RegisterWithGRPC implements agorapp.App.RegisterWithGRPC.
func (a *app) RegisterWithGRPC(server *grpc.Server) {
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
	})
}

func main() {
	if err := agoraapp.Run(&app{}); err != nil {
		logrus.WithError(err).Fatal("error running service")
	}
}
