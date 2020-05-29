package webhook

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/app"
	"github.com/kinecosystem/agora/pkg/webhook/common"
	"github.com/kinecosystem/agora/pkg/webhook/signtransaction"
)

var (
	emptyAcc      xdr.Uint256
	emptyEnvelope = xdr.TransactionEnvelope{
		Tx: xdr.Transaction{
			SourceAccount: xdr.AccountId{
				Type:    xdr.PublicKeyTypePublicKeyTypeEd25519,
				Ed25519: &emptyAcc,
			},
			Operations: []xdr.Operation{
				{
					Body: xdr.OperationBody{
						Type: xdr.OperationTypePayment,
						PaymentOp: &xdr.PaymentOp{
							Destination: xdr.AccountId{
								Type:    xdr.PublicKeyTypePublicKeyTypeEd25519,
								Ed25519: &emptyAcc,
							},
						},
					},
				},
			},
		},
	}
	basicReq = &signtransaction.RequestBody{
		EnvelopeXDR: "sometx",
		InvoiceList: "someinvoice",
	}
)

func TestSendSignTransactionRequest_AppURLNotSet(t *testing.T) {
	client := NewClient(http.DefaultClient)

	envelopeBytes, err := emptyEnvelope.MarshalBinary()
	require.NoError(t, err)
	expectedXDR := base64.StdEncoding.EncodeToString(envelopeBytes)

	config := &app.Config{
		AppName: "some name",
	}

	actualXDR, err := client.SignTransaction(context.Background(), config, &signtransaction.RequestBody{
		EnvelopeXDR: common.EnvelopeXDR(expectedXDR),
		InvoiceList: "someinvoice",
	})
	require.NoError(t, err)
	assert.Equal(t, expectedXDR, actualXDR)
}

func TestSendSignTransactionRequest_200Valid(t *testing.T) {
	client := NewClient(http.DefaultClient)

	envelopeBytes, err := emptyEnvelope.MarshalBinary()
	require.NoError(t, err)
	expectedXDR := base64.StdEncoding.EncodeToString(envelopeBytes)
	webhookResp := &signtransaction.SuccessResponse{EnvelopeXDR: common.EnvelopeXDR(expectedXDR)}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)

	testServer := newTestServerWithJSONResponse(t, 200, b)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	config := &app.Config{
		AppName:            "some name",
		SignTransactionURL: signURL,
	}

	actualXDR, err := client.SignTransaction(context.Background(), config, basicReq)
	require.NoError(t, err)
	assert.Equal(t, expectedXDR, actualXDR)
}

func TestSendSignTransactionRequest_200Invalid(t *testing.T) {
	client := NewClient(http.DefaultClient)

	testServer := newTestServerWithJSONResponse(t, 200, make([]byte, 0))
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	config := &app.Config{
		AppName:            "some name",
		SignTransactionURL: signURL,
	}

	actualXDR, err := client.SignTransaction(context.Background(), config, basicReq)

	require.Error(t, err)
	require.Empty(t, actualXDR)
}

func TestSendSignTransactionRequest_400Valid(t *testing.T) {
	client := NewClient(http.DefaultClient)

	webhookResp := &signtransaction.BadRequestResponse{Message: "some message"}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)

	testServer := newTestServerWithJSONResponse(t, 400, b)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	config := &app.Config{
		AppName:            "some name",
		SignTransactionURL: signURL,
	}

	actualXDR, err := client.SignTransaction(context.Background(), config, basicReq)

	signTxErr, ok := err.(*SignTransactionError)
	assert.True(t, ok)
	assert.Equal(t, 400, signTxErr.StatusCode)
	assert.Equal(t, webhookResp.Message, signTxErr.Message)
	require.Empty(t, actualXDR)
}

func TestSendSignTransactionRequest_400Invalid(t *testing.T) {
	client := NewClient(http.DefaultClient)

	testServer := newTestServerWithJSONResponse(t, 400, make([]byte, 0))
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	config := &app.Config{
		AppName:            "some name",
		SignTransactionURL: signURL,
	}

	actualXDR, err := client.SignTransaction(context.Background(), config, basicReq)

	require.Error(t, err)
	require.Empty(t, actualXDR)
}

func TestSendSignTransactionRequest_403Valid(t *testing.T) {
	client := NewClient(http.DefaultClient)

	webhookResp := &signtransaction.ForbiddenResponse{
		Message: "some message",
		InvoiceErrors: []signtransaction.InvoiceError{
			{
				OperationIndex: 0,
				Reason:         signtransaction.AlreadyPaid,
			},
		},
	}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)

	testServer := newTestServerWithJSONResponse(t, 403, b)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	config := &app.Config{
		AppName:            "some name",
		SignTransactionURL: signURL,
	}

	actualXDR, err := client.SignTransaction(context.Background(), config, basicReq)

	signTxErr, ok := err.(*SignTransactionError)
	assert.True(t, ok)
	assert.Equal(t, 403, signTxErr.StatusCode)
	assert.Equal(t, webhookResp.Message, signTxErr.Message)
	assert.Equal(t, webhookResp.InvoiceErrors, signTxErr.OperationErrors)
	require.Empty(t, actualXDR)
}

func TestSendSignTransactionRequest_403Invalid(t *testing.T) {
	client := NewClient(http.DefaultClient)

	testServer := newTestServerWithJSONResponse(t, 403, make([]byte, 0))
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	config := &app.Config{
		AppName:            "some name",
		SignTransactionURL: signURL,
	}

	actualXDR, err := client.SignTransaction(context.Background(), config, basicReq)

	require.Error(t, err)
	require.Empty(t, actualXDR)
}

func TestSendSignTransactionRequest_404Valid(t *testing.T) {
	client := NewClient(http.DefaultClient)

	webhookResp := &signtransaction.NotFoundResponse{Message: "some message"}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)

	testServer := newTestServerWithJSONResponse(t, 404, b)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	config := &app.Config{
		AppName:            "some name",
		SignTransactionURL: signURL,
	}

	actualXDR, err := client.SignTransaction(context.Background(), config, basicReq)

	signTxErr, ok := err.(*SignTransactionError)
	assert.True(t, ok)
	assert.Equal(t, 404, signTxErr.StatusCode)
	assert.Equal(t, webhookResp.Message, signTxErr.Message)
	require.Empty(t, actualXDR)
}

func TestSendSignTransactionRequest_404Invalid(t *testing.T) {
	client := NewClient(http.DefaultClient)

	testServer := newTestServerWithJSONResponse(t, 404, make([]byte, 0))
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	config := &app.Config{
		AppName:            "some name",
		SignTransactionURL: signURL,
	}

	actualXDR, err := client.SignTransaction(context.Background(), config, basicReq)

	require.Error(t, err)
	require.Empty(t, actualXDR)
}

func TestSendSignTransactionRequest_OtherStatusCode(t *testing.T) {
	client := NewClient(http.DefaultClient)

	testServer := newTestServerWithJSONResponse(t, 500, make([]byte, 0))
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)
	config := &app.Config{
		AppName:            "some name",
		SignTransactionURL: signURL,
	}

	actualXDR, err := client.SignTransaction(context.Background(), config, basicReq)
	signTxErr, ok := err.(*SignTransactionError)
	assert.True(t, ok)
	assert.Equal(t, 500, signTxErr.StatusCode)
	require.Empty(t, actualXDR)
}

func newTestServerWithJSONResponse(t *testing.T, statusCode int, b []byte) *httptest.Server {
	testServer := httptest.NewServer(http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		require.Equal(t, http.MethodPost, req.Method)
		resp.WriteHeader(statusCode)
		resp.Header().Set("Content-Type", "application/json")
		_, err := resp.Write(b)
		require.NoError(t, err)
	}))
	return testServer
}
