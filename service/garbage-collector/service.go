package main

import (
	"context"
	"crypto/ed25519"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/endpoints"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	agoraapp "github.com/kinecosystem/agora-common/app"
	"github.com/kinecosystem/agora-common/headers"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	sqstasks "github.com/kinecosystem/agora-common/taskqueue/sqs"
	"github.com/kinecosystem/go/strkey"
	"github.com/mr-tron/base58"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/ybbus/jsonrpc"
	xrate "golang.org/x/time/rate"
	"google.golang.org/grpc"

	"github.com/kinecosystem/agora/pkg/gc"
	gcpb "github.com/kinecosystem/agora/pkg/gc/proto"
	keypairdb "github.com/kinecosystem/agora/pkg/keypair"
	historyrw "github.com/kinecosystem/agora/pkg/transaction/history/dynamodb"

	// Configurable keystore options:
	_ "github.com/kinecosystem/agora/pkg/keypair/dynamodb"
	_ "github.com/kinecosystem/agora/pkg/keypair/environment"
	_ "github.com/kinecosystem/agora/pkg/keypair/memory"
)

const (
	keystoreTypeEnv = "KEYSTORE_TYPE"

	// Solana config
	solanaEndpointEnv      = "SOLANA_ENDPOINT"
	kinTokenEnv            = "KIN_TOKEN"
	subsidizerKeypairIDEnv = "SUBSIDIZER_KEYPAIR_ID"

	// Concurrency configs
	concurrencyEnv = "CONCURRENT_PROCESSORS"
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
	processor *gc.Processor

	shutdown   sync.Once
	shutdownCh chan struct{}
}

// Init implements agorapp.App.Init.
func (a *app) Init(_ agoraapp.Config) error {
	a.shutdownCh = make(chan struct{})

	if os.Getenv(solanaEndpointEnv) == "" {
		return errors.New("must specify solana endpoint")
	}
	if os.Getenv(subsidizerKeypairIDEnv) == "" {
		return errors.New("must specify subsidizer keypair id")
	}

	var err error
	var concurrentProcessors uint64 = 4
	if os.Getenv(concurrencyEnv) != "" {
		concurrentProcessors, err = strconv.ParseUint(os.Getenv(concurrencyEnv), 10, 64)
		if err != nil {
			return errors.Wrap(err, "failed to parse task concurrency")
		}
	}

	// We disable SSL for CPU performance.
	//
	// Note: the keystore uses it's own config loader, which _does_ use
	//       a secure client. This is the one store we care about to be
	//       over a secure channel, as it contains keys. The rest is just
	//       public key usage, all of which is over a blockchain.
	awsResolver := endpoints.NewDefaultResolver()
	awsResolver.DisableSSL = true
	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		return errors.Wrap(err, "failed to init v2 aws sdk")
	}
	cfg.EndpointResolver = awsResolver

	historyRW := historyrw.New(dynamodb.New(cfg))

	keystoreType := os.Getenv(keystoreTypeEnv)
	keystore, err := keypairdb.CreateStore(keystoreType)
	if err != nil {
		return errors.Wrapf(err, "failed to create keystore using configured : %s", keystoreType)
	}

	solanaClient := solana.NewWithRPCOptions(
		os.Getenv(solanaEndpointEnv),
		&jsonrpc.RPCClientOpts{
			HTTPClient: &http.Client{
				Transport: transport,
				Timeout:   30 * time.Second,
			},
		},
	)
	kinToken, err := base58.Decode(os.Getenv(kinTokenEnv))
	if err != nil {
		return errors.Wrap(err, "failed to parse kin token address")
	}
	if len(kinToken) != ed25519.PublicKeySize {
		return errors.Errorf("invalid kin token size: %d", len(kinToken))
	}

	subsidizerKP, err := keystore.Get(context.Background(), os.Getenv(subsidizerKeypairIDEnv))
	if err != nil {
		return errors.Wrap(err, "failed to determine subsidizer keypair")
	}
	rawSeed, err := strkey.Decode(strkey.VersionByteSeed, subsidizerKP.Seed())
	if err != nil {
		return errors.Wrap(err, "invalid subsidizer seed string")
	}
	subsidizer := ed25519.NewKeyFromSeed(rawSeed)

	sqsClient := sqs.New(cfg)
	a.processor, err = gc.NewProcessor(
		sqstasks.NewProcessorCtor(
			gc.GCQueueName,
			sqsClient,
			sqstasks.WithVisibilityTimeout(time.Minute),
			sqstasks.WithMaxVisibilityExtensions(5),
			sqstasks.WithVisibilityExtensionEnabled(true),
			sqstasks.WithTaskConcurrency(int(concurrentProcessors)),
			sqstasks.WithPausedStart(),
		),
		historyRW,
		solanaClient,
		token.NewClient(solanaClient, kinToken),
		subsidizer,
		xrate.NewLimiter(xrate.Limit(0), 0),
	)
	if err != nil {
		return errors.Wrap(err, "failed to create processor")
	}

	return nil
}

// RegisterWithGRPC implements agorapp.App.RegisterWithGRPC.
func (a *app) RegisterWithGRPC(server *grpc.Server) {
	gcpb.RegisterAdminServer(server, a.processor)
}

// ShutdownChan implements agorapp.App.ShutdownChan.
func (a *app) ShutdownChan() <-chan struct{} {
	return a.shutdownCh
}

// Stop implements agorapp.App.Stop.
func (a *app) Stop() {
	a.shutdown.Do(func() {
		close(a.shutdownCh)
		a.processor.Shutdown()
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
