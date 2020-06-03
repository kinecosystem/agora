package webhook

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/kinecosystem/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora-common/headers"
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
	appUserID      = "someuserid"
	appUserPasskey = "somepasskey"
	ctxWithHeaders = context.WithValue(
		context.Background(),
		headers.HeaderKey("ascii-header"),
		headers.Headers{
			AppUserIDHeader:      appUserID,
			AppUserPasskeyHeader: appUserPasskey,
		},
	)
)

func TestSendSignTransactionRequest_200Valid(t *testing.T) {
	client := NewClient(http.DefaultClient)

	envelopeBytes, err := emptyEnvelope.MarshalBinary()
	require.NoError(t, err)
	expectedXDR := base64.StdEncoding.EncodeToString(envelopeBytes)
	webhookResp := &signtransaction.SuccessResponse{EnvelopeXDR: expectedXDR}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)

	testServer := newTestServerWithJSONResponse(t, 200, b, appUserID, appUserPasskey, *basicReq)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	actualXDR, actualEnvelope, err := client.SignTransaction(ctxWithHeaders, *signURL, basicReq)
	require.NoError(t, err)
	assert.Equal(t, expectedXDR, actualXDR)
	assert.NotNil(t, actualEnvelope)
}

func TestSendSignTransactionRequest_200Invalid(t *testing.T) {
	client := NewClient(http.DefaultClient)

	testServer := newTestServerWithJSONResponse(t, 200, make([]byte, 0), appUserID, appUserPasskey, *basicReq)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	actualXDR, actualEnvelope, err := client.SignTransaction(ctxWithHeaders, *signURL, basicReq)

	require.Error(t, err)
	require.Empty(t, actualXDR)
	assert.Nil(t, actualEnvelope)
}

func TestSendSignTransactionRequest_400Valid(t *testing.T) {
	client := NewClient(http.DefaultClient)

	webhookResp := &signtransaction.BadRequestResponse{Message: "some message"}
	b, err := json.Marshal(webhookResp)

	require.NoError(t, err)

	testServer := newTestServerWithJSONResponse(t, 400, b, appUserID, appUserPasskey, *basicReq)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	actualXDR, actualEnvelope, err := client.SignTransaction(ctxWithHeaders, *signURL, basicReq)

	signTxErr, ok := err.(*SignTransactionError)
	assert.True(t, ok)
	assert.Equal(t, 400, signTxErr.StatusCode)
	assert.Equal(t, webhookResp.Message, signTxErr.Message)
	require.Empty(t, actualXDR)
	assert.Nil(t, actualEnvelope)
}

func TestSendSignTransactionRequest_400Invalid(t *testing.T) {
	client := NewClient(http.DefaultClient)

	testServer := newTestServerWithJSONResponse(t, 400, make([]byte, 0), appUserID, appUserPasskey, *basicReq)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	actualXDR, actualEnvelope, err := client.SignTransaction(ctxWithHeaders, *signURL, basicReq)

	require.Error(t, err)
	require.Empty(t, actualXDR)
	assert.Nil(t, actualEnvelope)
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

	testServer := newTestServerWithJSONResponse(t, 403, b, appUserID, appUserPasskey, *basicReq)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	actualXDR, actualEnvelope, err := client.SignTransaction(ctxWithHeaders, *signURL, basicReq)

	signTxErr, ok := err.(*SignTransactionError)
	assert.True(t, ok)
	assert.Equal(t, 403, signTxErr.StatusCode)
	assert.Equal(t, webhookResp.Message, signTxErr.Message)
	assert.Equal(t, webhookResp.InvoiceErrors, signTxErr.OperationErrors)
	require.Empty(t, actualXDR)
	assert.Nil(t, actualEnvelope)
}

func TestSendSignTransactionRequest_403Invalid(t *testing.T) {
	client := NewClient(http.DefaultClient)

	testServer := newTestServerWithJSONResponse(t, 403, make([]byte, 0), appUserID, appUserPasskey, *basicReq)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	actualXDR, actualEnvelope, err := client.SignTransaction(ctxWithHeaders, *signURL, basicReq)

	require.Error(t, err)
	require.Empty(t, actualXDR)
	assert.Nil(t, actualEnvelope)
}

func TestSendSignTransactionRequest_OtherStatusCode(t *testing.T) {
	client := NewClient(http.DefaultClient)

	testServer := newTestServerWithJSONResponse(t, 500, make([]byte, 0), appUserID, appUserPasskey, *basicReq)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	actualXDR, actualEnvelope, err := client.SignTransaction(ctxWithHeaders, *signURL, basicReq)
	signTxErr, ok := err.(*SignTransactionError)
	assert.True(t, ok)
	assert.Equal(t, 500, signTxErr.StatusCode)
	require.Empty(t, actualXDR)
	assert.Nil(t, actualEnvelope)
}

func TestSendSignTransactionRequest_NoAuthHeaders(t *testing.T) {
	client := NewClient(http.DefaultClient)

	envelopeBytes, err := emptyEnvelope.MarshalBinary()
	require.NoError(t, err)
	expectedXDR := base64.StdEncoding.EncodeToString(envelopeBytes)
	webhookResp := &signtransaction.SuccessResponse{EnvelopeXDR: expectedXDR}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)

	testServer := newTestServerWithJSONResponse(t, 200, b, "", "", *basicReq)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	ctxWithEmptyHeaders := context.WithValue(
		context.Background(),
		headers.HeaderKey("ascii-header"),
		headers.Headers{},
	)

	actualXDR, actualEnvelope, err := client.SignTransaction(ctxWithEmptyHeaders, *signURL, basicReq)
	require.NoError(t, err)
	assert.Equal(t, expectedXDR, actualXDR)
	assert.NotNil(t, actualEnvelope)
}

func TestSendSignTransactionRequest_InvalidHeaders(t *testing.T) {
	client := NewClient(http.DefaultClient)

	signURL, err := url.Parse("www.webhook.com")
	require.NoError(t, err)

	// Context missing Agora headers
	actualXDR, actualEnvelope, err := client.SignTransaction(context.Background(), *signURL, basicReq)
	require.Error(t, err)
	assert.Empty(t, actualXDR)
	assert.Nil(t, actualEnvelope)

	// Context with missing userID
	ctx := context.WithValue(
		context.Background(),
		headers.HeaderKey("ascii-header"),
		headers.Headers{
			AppUserPasskeyHeader: appUserPasskey,
		},
	)

	actualXDR, actualEnvelope, err = client.SignTransaction(ctx, *signURL, basicReq)
	require.Error(t, err)
	assert.Empty(t, actualXDR)
	assert.Nil(t, actualEnvelope)

	// Context with missing passkey
	ctx = context.WithValue(
		context.Background(),
		headers.HeaderKey("ascii-header"),
		headers.Headers{
			AppUserIDHeader: appUserID,
		},
	)

	actualXDR, actualEnvelope, err = client.SignTransaction(ctx, *signURL, basicReq)
	require.Error(t, err)
	assert.Empty(t, actualXDR)
	assert.Nil(t, actualEnvelope)
}

func newTestServerWithJSONResponse(t *testing.T, statusCode int, b []byte, expectedUserID string, expectedPasskey string, expectedReq signtransaction.RequestBody) *httptest.Server {
	testServer := httptest.NewServer(http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		require.Equal(t, http.MethodPost, req.Method)
		require.Equal(t, expectedUserID, req.Header.Get("X-App-User-ID"))
		require.Equal(t, expectedPasskey, req.Header.Get("X-App-User-Passkey"))
		actualReq := &signtransaction.RequestBody{}
		err := json.NewDecoder(req.Body).Decode(actualReq)
		require.NoError(t, err)
		require.Equal(t, expectedReq, *actualReq)

		resp.WriteHeader(statusCode)
		resp.Header().Set("Content-Type", "application/json")
		_, err = resp.Write(b)
		require.NoError(t, err)
	}))
	return testServer
}
