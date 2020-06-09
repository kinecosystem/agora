package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io/ioutil"
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
			AppUserIDCtxHeader:      appUserID,
			AppUserPasskeyCtxHeader: appUserPasskey,
		},
	)
)

type testEnv struct {
	client    *Client
	secretKey []byte
}

func setup(t *testing.T) (env testEnv) {
	env.secretKey = make([]byte, 32)
	_, err := rand.Read(env.secretKey)
	require.NoError(t, err)

	env.client = NewClient(http.DefaultClient)

	return env
}

func TestSendSignTransactionRequest_InvalidWebhookSecret(t *testing.T) {
	env := setup(t)

	signURL, err := url.Parse("www.webhook.com")
	require.NoError(t, err)

	actualXDR, actualEnvelope, err := env.client.SignTransaction(ctxWithHeaders, *signURL, make([]byte, 0), basicReq)
	require.Error(t, err)
	assert.Empty(t, actualXDR)
	assert.Nil(t, actualEnvelope)
}

func TestSendSignTransactionRequest_200Valid(t *testing.T) {
	env := setup(t)

	envelopeBytes, err := emptyEnvelope.MarshalBinary()
	require.NoError(t, err)
	expectedXDR := base64.StdEncoding.EncodeToString(envelopeBytes)
	webhookResp := &signtransaction.SuccessResponse{EnvelopeXDR: expectedXDR}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)

	testServer := newTestServerWithJSONResponse(t, env, 200, b, appUserID, appUserPasskey, *basicReq)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	actualXDR, actualEnvelope, err := env.client.SignTransaction(ctxWithHeaders, *signURL, env.secretKey, basicReq)
	require.NoError(t, err)
	assert.Equal(t, expectedXDR, actualXDR)
	assert.NotNil(t, actualEnvelope)
}

func TestSendSignTransactionRequest_200Invalid(t *testing.T) {
	env := setup(t)

	testServer := newTestServerWithJSONResponse(t, env, 200, make([]byte, 0), appUserID, appUserPasskey, *basicReq)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	actualXDR, actualEnvelope, err := env.client.SignTransaction(ctxWithHeaders, *signURL, env.secretKey, basicReq)

	require.Error(t, err)
	require.Empty(t, actualXDR)
	assert.Nil(t, actualEnvelope)
}

func TestSendSignTransactionRequest_400Valid(t *testing.T) {
	env := setup(t)

	webhookResp := &signtransaction.BadRequestResponse{Message: "some message"}
	b, err := json.Marshal(webhookResp)

	require.NoError(t, err)

	testServer := newTestServerWithJSONResponse(t, env, 400, b, appUserID, appUserPasskey, *basicReq)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	actualXDR, actualEnvelope, err := env.client.SignTransaction(ctxWithHeaders, *signURL, env.secretKey, basicReq)

	signTxErr, ok := err.(*SignTransactionError)
	assert.True(t, ok)
	assert.Equal(t, 400, signTxErr.StatusCode)
	assert.Equal(t, webhookResp.Message, signTxErr.Message)
	require.Empty(t, actualXDR)
	assert.Nil(t, actualEnvelope)
}

func TestSendSignTransactionRequest_400Invalid(t *testing.T) {
	env := setup(t)

	testServer := newTestServerWithJSONResponse(t, env, 400, make([]byte, 0), appUserID, appUserPasskey, *basicReq)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	actualXDR, actualEnvelope, err := env.client.SignTransaction(ctxWithHeaders, *signURL, env.secretKey, basicReq)

	require.Error(t, err)
	require.Empty(t, actualXDR)
	assert.Nil(t, actualEnvelope)
}

func TestSendSignTransactionRequest_403Valid(t *testing.T) {
	env := setup(t)

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

	testServer := newTestServerWithJSONResponse(t, env, 403, b, appUserID, appUserPasskey, *basicReq)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	actualXDR, actualEnvelope, err := env.client.SignTransaction(ctxWithHeaders, *signURL, env.secretKey, basicReq)

	signTxErr, ok := err.(*SignTransactionError)
	assert.True(t, ok)
	assert.Equal(t, 403, signTxErr.StatusCode)
	assert.Equal(t, webhookResp.Message, signTxErr.Message)
	assert.Equal(t, webhookResp.InvoiceErrors, signTxErr.OperationErrors)
	require.Empty(t, actualXDR)
	assert.Nil(t, actualEnvelope)
}

func TestSendSignTransactionRequest_403Invalid(t *testing.T) {
	env := setup(t)

	testServer := newTestServerWithJSONResponse(t, env, 403, make([]byte, 0), appUserID, appUserPasskey, *basicReq)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	actualXDR, actualEnvelope, err := env.client.SignTransaction(ctxWithHeaders, *signURL, env.secretKey, basicReq)

	require.Error(t, err)
	require.Empty(t, actualXDR)
	assert.Nil(t, actualEnvelope)
}

func TestSendSignTransactionRequest_OtherStatusCode(t *testing.T) {
	env := setup(t)

	testServer := newTestServerWithJSONResponse(t, env, 500, make([]byte, 0), appUserID, appUserPasskey, *basicReq)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	actualXDR, actualEnvelope, err := env.client.SignTransaction(ctxWithHeaders, *signURL, env.secretKey, basicReq)
	signTxErr, ok := err.(*SignTransactionError)
	assert.True(t, ok)
	assert.Equal(t, 500, signTxErr.StatusCode)
	require.Empty(t, actualXDR)
	assert.Nil(t, actualEnvelope)
}

func TestSendSignTransactionRequest_NoAuthHeaders(t *testing.T) {
	env := setup(t)

	envelopeBytes, err := emptyEnvelope.MarshalBinary()
	require.NoError(t, err)
	expectedXDR := base64.StdEncoding.EncodeToString(envelopeBytes)
	webhookResp := &signtransaction.SuccessResponse{EnvelopeXDR: expectedXDR}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)

	testServer := newTestServerWithJSONResponse(t, env, 200, b, "", "", *basicReq)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	ctxWithEmptyHeaders := context.WithValue(
		context.Background(),
		headers.HeaderKey("ascii-header"),
		headers.Headers{},
	)

	actualXDR, actualEnvelope, err := env.client.SignTransaction(ctxWithEmptyHeaders, *signURL, env.secretKey, basicReq)
	require.NoError(t, err)
	assert.Equal(t, expectedXDR, actualXDR)
	assert.NotNil(t, actualEnvelope)
}

func TestSendSignTransactionRequest_InvalidHeaders(t *testing.T) {
	env := setup(t)

	signURL, err := url.Parse("www.webhook.com")
	require.NoError(t, err)

	// Context missing Agora headers
	actualXDR, actualEnvelope, err := env.client.SignTransaction(context.Background(), *signURL, env.secretKey, basicReq)
	require.Error(t, err)
	assert.Empty(t, actualXDR)
	assert.Nil(t, actualEnvelope)

	// Context with missing userID
	ctx := context.WithValue(
		context.Background(),
		headers.HeaderKey("ascii-header"),
		headers.Headers{
			AppUserPasskeyCtxHeader: appUserPasskey,
		},
	)

	actualXDR, actualEnvelope, err = env.client.SignTransaction(ctx, *signURL, env.secretKey, basicReq)
	require.Error(t, err)
	assert.Empty(t, actualXDR)
	assert.Nil(t, actualEnvelope)

	// Context with missing passkey
	ctx = context.WithValue(
		context.Background(),
		headers.HeaderKey("ascii-header"),
		headers.Headers{
			AppUserIDCtxHeader: appUserID,
		},
	)

	actualXDR, actualEnvelope, err = env.client.SignTransaction(ctx, *signURL, env.secretKey, basicReq)
	require.Error(t, err)
	assert.Empty(t, actualXDR)
	assert.Nil(t, actualEnvelope)
}

func newTestServerWithJSONResponse(t *testing.T, env testEnv, statusCode int, b []byte, expectedUserID string, expectedPasskey string, expectedReq signtransaction.RequestBody) *httptest.Server {
	testServer := httptest.NewServer(http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		require.Equal(t, http.MethodPost, req.Method)
		require.Equal(t, expectedUserID, req.Header.Get(AppUserIDHeader))
		require.Equal(t, expectedPasskey, req.Header.Get(AppUserPasskeyHeader))

		body, err := ioutil.ReadAll(req.Body)
		require.NoError(t, err)

		agoraSignature, err := base64.StdEncoding.DecodeString(req.Header.Get(AgoraHMACHeader))
		require.NoError(t, err)

		h := hmac.New(sha256.New, env.secretKey)
		_, err = h.Write(body)
		require.NoError(t, err)

		sig := h.Sum(nil)
		assert.True(t, hmac.Equal(sig, agoraSignature))

		actualReq := &signtransaction.RequestBody{}
		err = json.Unmarshal(body, actualReq)
		require.NoError(t, err)
		require.Equal(t, expectedReq, *actualReq)

		resp.WriteHeader(statusCode)
		resp.Header().Set("Content-Type", "application/json")
		_, err = resp.Write(b)
		require.NoError(t, err)
	}))
	return testServer
}
