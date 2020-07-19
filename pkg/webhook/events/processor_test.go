package events

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/sqsiface"
	"github.com/golang/protobuf/proto"
	sqstest "github.com/kinecosystem/agora-common/aws/sqs/test"
	"github.com/kinecosystem/agora-common/taskqueue/model/task"
	sqstasks "github.com/kinecosystem/agora-common/taskqueue/sqs"
	"github.com/ory/dockertest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"

	"github.com/kinecosystem/agora/pkg/app"
	appmemory "github.com/kinecosystem/agora/pkg/app/memory"
	"github.com/kinecosystem/agora/pkg/invoice"
	invoicememory "github.com/kinecosystem/agora/pkg/invoice/memory"
	"github.com/kinecosystem/agora/pkg/testutil"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
	historytestutil "github.com/kinecosystem/agora/pkg/transaction/history/model/testutil"
	"github.com/kinecosystem/agora/pkg/webhook"
)

var (
	sqsClient sqsiface.ClientAPI

	il = &commonpb.InvoiceList{
		Invoices: []*commonpb.Invoice{
			{
				Items: []*commonpb.Invoice_LineItem{
					{
						Title:       "lineitem1",
						Description: "desc1",
						Amount:      5,
					},
				},
			},
		},
	}
)

type testEnv struct {
	processor      *Processor
	invoiceStore   invoice.Store
	appConfigStore app.ConfigStore
}

func TestMain(m *testing.M) {
	testPool, err := dockertest.NewPool("")
	if err != nil {
		panic("error creating docker pool: " + err.Error())
	}

	client, cleanupSqs, err := sqstest.StartLocalSQS(testPool)
	if err != nil {
		panic("error starting SQS image: " + err.Error())
	}
	sqsClient = client

	code := m.Run()
	cleanupSqs()
	os.Exit(code)
}

func setup(t *testing.T) (env testEnv, teardown func()) {
	env.invoiceStore = invoicememory.New()
	env.appConfigStore = appmemory.New()

	setupQueue(t, IngestionQueueName)
	teardown = func() { deleteQueue(t, IngestionQueueName) }

	queueCtor := sqstasks.NewProcessorCtor(
		IngestionQueueName,
		sqsClient,
	)

	p, err := NewProcessor(
		queueCtor,
		env.invoiceStore,
		env.appConfigStore,
		webhook.NewClient(http.DefaultClient),
	)
	require.NoError(t, err)
	env.processor = p

	return env, teardown
}

func TestRoundTrip(t *testing.T) {
	env, teardown := setup(t)
	defer teardown()

	ilBytes, err := proto.Marshal(il)
	require.NoError(t, err)
	ilHash := sha256.Sum224(ilBytes)

	accountIDs := testutil.GenerateAccountIDs(t, 2)
	entry, txHash := historytestutil.GenerateEntry(t, 10, 10, accountIDs[0], accountIDs[1:], ilHash[:])

	require.NoError(t, env.invoiceStore.Put(context.Background(), txHash, il))

	called := make(chan struct{})
	testServer := httptest.NewServer(http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		b, err := ioutil.ReadAll(req.Body)
		defer req.Body.Close()
		require.NoError(t, err)

		var events []Event
		require.NoError(t, json.Unmarshal(b, &events))

		assert.Len(t, events, 1)

		txEvent := events[0].TransactionEvent
		assert.NotNil(t, txEvent)
		assert.EqualValues(t, model.KinVersion_KIN3, txEvent.KinVersion)
		assert.EqualValues(t, txHash, txEvent.TxHash)
		assert.True(t, proto.Equal(il, txEvent.InvoiceList))

		assert.NotNil(t, txEvent.StellarEvent)
		assert.NotNil(t, entry.Kind.(*model.Entry_Stellar).Stellar.EnvelopeXdr, txEvent.StellarEvent.EnvelopeXDR)
		assert.NotNil(t, entry.Kind.(*model.Entry_Stellar).Stellar.ResultXdr, txEvent.StellarEvent.ResultXDR)

		close(called)
	}))

	eventsURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	appConfig := &app.Config{
		AppName:       "kin",
		EventsURL:     eventsURL,
		WebhookSecret: "secret",
	}
	err = env.appConfigStore.Add(context.Background(), 0, appConfig)
	require.NoError(t, err)

	require.NoError(t, env.processor.Write(context.Background(), entry))
	select {
	case <-called:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for webhook call")
	}
}

func TestWebhook_None(t *testing.T) {
	env, teardown := setup(t)
	defer teardown()

	ilBytes, err := proto.Marshal(il)
	require.NoError(t, err)
	ilHash := sha256.Sum224(ilBytes)

	accountIDs := testutil.GenerateAccountIDs(t, 2)
	entry, txHash := historytestutil.GenerateEntry(t, 10, 10, accountIDs[0], accountIDs[1:], ilHash[:])
	require.NoError(t, env.invoiceStore.Put(context.Background(), txHash, il))

	appConfig := &app.Config{
		AppName:       "kin",
		WebhookSecret: "secret",
	}
	err = env.appConfigStore.Add(context.Background(), 0, appConfig)
	require.NoError(t, err)

	b, err := proto.Marshal(entry)
	require.NoError(t, err)
	msg := &task.Message{
		TypeName: proto.MessageName(entry),
		RawValue: b,
	}
	require.NoError(t, env.processor.queueHandler(context.Background(), msg))
}

func TestWebhook_NonRetriableError(t *testing.T) {
	env, teardown := setup(t)
	defer teardown()

	ilBytes, err := proto.Marshal(il)
	require.NoError(t, err)
	ilHash := sha256.Sum224(ilBytes)

	accountIDs := testutil.GenerateAccountIDs(t, 2)
	entry, txHash := historytestutil.GenerateEntry(t, 10, 10, accountIDs[0], accountIDs[1:], ilHash[:])

	require.NoError(t, env.invoiceStore.Put(context.Background(), txHash, il))

	testServer := httptest.NewServer(http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		defer req.Body.Close()
		http.Error(resp, "", http.StatusBadRequest)
	}))

	eventsURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	appConfig := &app.Config{
		AppName:       "kin",
		EventsURL:     eventsURL,
		WebhookSecret: "secret",
	}
	err = env.appConfigStore.Add(context.Background(), 0, appConfig)
	require.NoError(t, err)

	b, err := proto.Marshal(entry)
	require.NoError(t, err)
	msg := &task.Message{
		TypeName: proto.MessageName(entry),
		RawValue: b,
	}
	assert.NoError(t, env.processor.queueHandler(context.Background(), msg))
}

func TestWebhook_RetriableError(t *testing.T) {
	env, teardown := setup(t)
	defer teardown()

	ilBytes, err := proto.Marshal(il)
	require.NoError(t, err)
	ilHash := sha256.Sum224(ilBytes)

	accountIDs := testutil.GenerateAccountIDs(t, 2)
	entry, txHash := historytestutil.GenerateEntry(t, 10, 10, accountIDs[0], accountIDs[1:], ilHash[:])

	require.NoError(t, env.invoiceStore.Put(context.Background(), txHash, il))

	testServer := httptest.NewServer(http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		defer req.Body.Close()

		http.Error(resp, "", http.StatusInternalServerError)
	}))

	eventsURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	appConfig := &app.Config{
		AppName:       "kin",
		EventsURL:     eventsURL,
		WebhookSecret: "secret",
	}
	err = env.appConfigStore.Add(context.Background(), 0, appConfig)
	require.NoError(t, err)

	b, err := proto.Marshal(entry)
	require.NoError(t, err)
	msg := &task.Message{
		TypeName: proto.MessageName(entry),
		RawValue: b,
	}
	assert.Error(t, env.processor.queueHandler(context.Background(), msg))
}

func setupQueue(t *testing.T, queueName string) string {
	resp, err := sqsClient.GetQueueUrlRequest(&sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	}).Send(context.Background())
	if err != nil {
		resp, err := sqsClient.CreateQueueRequest(&sqs.CreateQueueInput{
			QueueName: aws.String(queueName),
		}).Send(context.Background())
		require.NoError(t, err)
		return aws.StringValue(resp.QueueUrl)
	}
	return aws.StringValue(resp.QueueUrl)
}

func deleteQueue(t *testing.T, queueName string) {
	resp, err := sqsClient.GetQueueUrlRequest(&sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	}).Send(context.Background())
	require.NoError(t, err)

	// Clear out the queue
	_, err = sqsClient.DeleteQueueRequest(&sqs.DeleteQueueInput{
		QueueUrl: resp.QueueUrl,
	}).Send(context.Background())
	require.NoError(t, err)
}
