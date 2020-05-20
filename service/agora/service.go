package main

import (
	"context"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	agoraapp "github.com/kinecosystem/agora-common/app"
	"github.com/kinecosystem/agora-common/kin"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	accountpb "github.com/kinecosystem/kin-api-internal/genproto/account/v3"
	transactionpb "github.com/kinecosystem/kin-api-internal/genproto/transaction/v3"

	accountserver "github.com/kinecosystem/agora/pkg/account/server"
	appconfigdb "github.com/kinecosystem/agora/pkg/app/dynamodb"
	invoicedb "github.com/kinecosystem/agora/pkg/invoice/dynamodb"
	keypairdb "github.com/kinecosystem/agora/pkg/keypair"
	transactionserver "github.com/kinecosystem/agora/pkg/transaction/server"
)

const (
	rootKeypairIDEnv = "ROOT_KEYPAIR_ID"
	keystoreTypeEnv  = "KEYSTORE_TYPE"
)

type app struct {
	accountServer accountpb.AccountServer
	txnServer     transactionpb.TransactionServer

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
		return errors.Wrap(err, "failed to determine root accountpb keypair")
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
		return err
	}

	dynamoClient := dynamodb.New(cfg)
	appConfigStore := appconfigdb.New(dynamoClient)
	invoiceStore := invoicedb.New(dynamoClient)

	a.accountServer = accountserver.New(rootAccountKP, client)
	a.txnServer = transactionserver.New(
		appConfigStore,
		invoiceStore,
		client,
		clientV2,
	)

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
	})
}

func main() {
	if err := agoraapp.Run(&app{}); err != nil {
		logrus.WithError(err).Fatal("error running service")
	}
}
