package webhook

import (
	"context"
	"crypto/ed25519"
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

	"github.com/kinecosystem/agora-common/headers"
	"github.com/kinecosystem/agora-common/webhook/createaccount"
	"github.com/kinecosystem/agora-common/webhook/signtransaction"
	"github.com/kinecosystem/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	basicReq = &signtransaction.Request{
		SolanaTransaction: []byte{1},
		InvoiceList:       []byte{2},
	}
	basicReqBody   []byte
	appUserID      = "someuserid"
	appUserPasskey = "somepasskey"
	ctxWithHeaders = context.WithValue(
		context.Background(),
		headers.HeaderKey("ascii-header"),
		headers.Headers{
			appUserIDCtxHeader:      appUserID,
			appUserPasskeyCtxHeader: appUserPasskey,
		},
	)
)

type testEnv struct {
	client    *Client
	secretKey string
}

func setup(t *testing.T) (env testEnv) {
	secretKey := make([]byte, 16)
	_, err := rand.Read(secretKey)
	require.NoError(t, err)
	env.secretKey = string(secretKey)

	basicReqBody, err = json.Marshal(basicReq)
	require.NoError(t, err)

	env.client = NewClient(http.DefaultClient)

	return env
}

func TestCreateAccountRequest_InvalidWebhookSecret(t *testing.T) {
	env := setup(t)

	createAccountURL, err := url.Parse("www.webhook.com")
	require.NoError(t, err)

	result, err := env.client.CreateAccount(ctxWithHeaders, *createAccountURL, "", &createaccount.Request{
		KinVersion:        4,
		SolanaTransaction: []byte("test"),
	})
	require.Error(t, err)
	assert.Nil(t, result)
}

func TestCreateAccountRequest_200(t *testing.T) {
	env := setup(t)

	webhookResp := &createaccount.SuccessResponse{
		Signature: make([]byte, ed25519.SignatureSize),
	}
	_, err := rand.Read(webhookResp.Signature)
	require.NoError(t, err)
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)

	req := &createaccount.Request{
		KinVersion:        4,
		SolanaTransaction: make([]byte, 4),
	}
	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	testServer := newTestServerWithJSONResponse(t, env, 200, b, appUserID, appUserPasskey, reqBytes)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	resp, err := env.client.CreateAccount(ctxWithHeaders, *signURL, env.secretKey, req)
	require.NoError(t, err)

	assert.Equal(t, webhookResp.Signature, resp.Signature)
}
func TestCreateAccountRequest_403(t *testing.T) {
	env := setup(t)

	webhookResp := struct{}{}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)

	req := &createaccount.Request{
		KinVersion:        4,
		SolanaTransaction: make([]byte, 4),
	}
	reqBytes, err := json.Marshal(req)
	require.NoError(t, err)

	testServer := newTestServerWithJSONResponse(t, env, 403, b, appUserID, appUserPasskey, reqBytes)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	_, err = env.client.CreateAccount(ctxWithHeaders, *signURL, env.secretKey, req)
	assert.Error(t, err)

	createAccountErr, ok := err.(*CreateAccountError)
	assert.True(t, ok)
	assert.Equal(t, 403, createAccountErr.StatusCode)

}

func TestSendSignTransactionRequest_InvalidWebhookSecret(t *testing.T) {
	env := setup(t)

	signURL, err := url.Parse("www.webhook.com")
	require.NoError(t, err)

	actualEnvelope, err := env.client.SignTransaction(ctxWithHeaders, *signURL, "", basicReq)
	require.Error(t, err)
	assert.Nil(t, actualEnvelope)
}

func TestSendSignTransactionRequest_200Valid(t *testing.T) {
	env := setup(t)

	envelopeBytes, err := emptyEnvelope.MarshalBinary()
	require.NoError(t, err)
	webhookResp := &signtransaction.SuccessResponse{EnvelopeXDR: envelopeBytes}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)

	testServer := newTestServerWithJSONResponse(t, env, 200, b, appUserID, appUserPasskey, basicReqBody)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	resp, err := env.client.SignTransaction(ctxWithHeaders, *signURL, env.secretKey, basicReq)
	require.NoError(t, err)
	var actualEnvelope xdr.TransactionEnvelope
	require.NoError(t, actualEnvelope.UnmarshalBinary(resp.EnvelopeXDR))
	assert.EqualValues(t, emptyEnvelope, actualEnvelope)
}

func TestSendSignTransactionRequest_200Invalid(t *testing.T) {
	env := setup(t)

	testServer := newTestServerWithJSONResponse(t, env, 200, []byte{}, appUserID, appUserPasskey, basicReqBody)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	actualEnvelope, err := env.client.SignTransaction(ctxWithHeaders, *signURL, env.secretKey, basicReq)

	require.Error(t, err)
	assert.Nil(t, actualEnvelope)
}

func TestSendSignTransactionRequest_400Valid(t *testing.T) {
	env := setup(t)

	testServer := newTestServerWithJSONResponse(t, env, 400, []byte{}, appUserID, appUserPasskey, basicReqBody)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	actualEnvelope, err := env.client.SignTransaction(ctxWithHeaders, *signURL, env.secretKey, basicReq)

	signTxErr, ok := err.(*SignTransactionError)
	assert.True(t, ok)
	assert.Equal(t, 400, signTxErr.StatusCode)
	assert.Nil(t, actualEnvelope)
}

func TestSendSignTransactionRequest_400Invalid(t *testing.T) {
	env := setup(t)

	testServer := newTestServerWithJSONResponse(t, env, 400, []byte{}, appUserID, appUserPasskey, basicReqBody)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	actualEnvelope, err := env.client.SignTransaction(ctxWithHeaders, *signURL, env.secretKey, basicReq)

	require.Error(t, err)
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

	testServer := newTestServerWithJSONResponse(t, env, 403, b, appUserID, appUserPasskey, basicReqBody)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	actualEnvelope, err := env.client.SignTransaction(ctxWithHeaders, *signURL, env.secretKey, basicReq)

	signTxErr, ok := err.(*SignTransactionError)
	assert.True(t, ok)
	assert.Equal(t, 403, signTxErr.StatusCode)
	assert.Equal(t, webhookResp.Message, signTxErr.Message)
	assert.Equal(t, webhookResp.InvoiceErrors, signTxErr.OperationErrors)
	assert.Nil(t, actualEnvelope)
}

func TestSendSignTransactionRequest_403Invalid(t *testing.T) {
	env := setup(t)

	testServer := newTestServerWithJSONResponse(t, env, 403, []byte{}, appUserID, appUserPasskey, basicReqBody)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	actualEnvelope, err := env.client.SignTransaction(ctxWithHeaders, *signURL, env.secretKey, basicReq)

	require.Error(t, err)
	assert.Nil(t, actualEnvelope)
}

func TestSendSignTransactionRequest_OtherStatusCode(t *testing.T) {
	env := setup(t)

	testServer := newTestServerWithJSONResponse(t, env, 500, []byte{}, appUserID, appUserPasskey, basicReqBody)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	actualEnvelope, err := env.client.SignTransaction(ctxWithHeaders, *signURL, env.secretKey, basicReq)
	signTxErr, ok := err.(*SignTransactionError)
	assert.True(t, ok)
	assert.Equal(t, 500, signTxErr.StatusCode)
	assert.Nil(t, actualEnvelope)
}

func TestSendSignTransactionRequest_NoAuthHeaders(t *testing.T) {
	env := setup(t)

	envelopeBytes, err := emptyEnvelope.MarshalBinary()
	require.NoError(t, err)
	webhookResp := &signtransaction.SuccessResponse{EnvelopeXDR: envelopeBytes}
	b, err := json.Marshal(webhookResp)
	require.NoError(t, err)

	testServer := newTestServerWithJSONResponse(t, env, 200, b, "", "", basicReqBody)
	defer func() { testServer.Close() }()

	signURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	ctxWithEmptyHeaders := context.WithValue(
		context.Background(),
		headers.HeaderKey("ascii-header"),
		headers.Headers{},
	)

	resp, err := env.client.SignTransaction(ctxWithEmptyHeaders, *signURL, env.secretKey, basicReq)
	require.NoError(t, err)
	var actualEnvelope xdr.TransactionEnvelope
	require.NoError(t, actualEnvelope.UnmarshalBinary(resp.EnvelopeXDR))
	assert.Equal(t, emptyEnvelope, actualEnvelope)
}

func TestSendSignTransactionRequest_InvalidHeaders(t *testing.T) {
	env := setup(t)

	signURL, err := url.Parse("www.webhook.com")
	require.NoError(t, err)

	// Context missing Agora headers
	actualEnvelope, err := env.client.SignTransaction(context.Background(), *signURL, env.secretKey, basicReq)
	require.Error(t, err)
	assert.Nil(t, actualEnvelope)

	// Context with missing userID
	ctx := context.WithValue(
		context.Background(),
		headers.HeaderKey("ascii-header"),
		headers.Headers{
			appUserPasskeyCtxHeader: appUserPasskey,
		},
	)

	actualEnvelope, err = env.client.SignTransaction(ctx, *signURL, env.secretKey, basicReq)
	require.Error(t, err)
	assert.Nil(t, actualEnvelope)

	// Context with missing passkey
	ctx = context.WithValue(
		context.Background(),
		headers.HeaderKey("ascii-header"),
		headers.Headers{
			appUserIDCtxHeader: appUserID,
		},
	)

	actualEnvelope, err = env.client.SignTransaction(ctx, *signURL, env.secretKey, basicReq)
	require.Error(t, err)
	assert.Nil(t, actualEnvelope)
}

func TestSendEventsRequest_InvalidWebhookSecret(t *testing.T) {
	env := setup(t)

	eventsURL, err := url.Parse("www.webhook.com")
	require.NoError(t, err)

	require.Error(t, env.client.Events(ctxWithHeaders, *eventsURL, "", []byte("{}")))
}

func TestSendEventsRequest_StatusCodes(t *testing.T) {
	env := setup(t)

	for _, code := range []int{200, 300, 400} {
		testServer := newTestServerWithJSONResponse(t, env, code, []byte{}, "", "", []byte("{}"))
		eventsURL, err := url.Parse(testServer.URL)
		require.NoError(t, err)

		err = env.client.Events(context.Background(), *eventsURL, env.secretKey, []byte("{}"))
		testServer.Close()

		assert.NoError(t, err)
	}
	testServer := newTestServerWithJSONResponse(t, env, 500, []byte{}, "", "", []byte("{}"))
	defer testServer.Close()

	eventsURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	assert.Error(t, env.client.Events(context.Background(), *eventsURL, env.secretKey, []byte("{}")))
}

func newTestServerWithJSONResponse(t *testing.T, env testEnv, statusCode int, respBody []byte, expectedUserID string, expectedPasskey string, expectedReq []byte) *httptest.Server {
	testServer := httptest.NewServer(http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		require.Equal(t, http.MethodPost, req.Method)
		require.Equal(t, expectedUserID, req.Header.Get(appUserIDHeader))
		require.Equal(t, expectedPasskey, req.Header.Get(appUserPasskeyHeader))

		body, err := ioutil.ReadAll(req.Body)
		require.NoError(t, err)

		agoraSignature, err := base64.StdEncoding.DecodeString(req.Header.Get(agoraHMACHeader))
		require.NoError(t, err)

		h := hmac.New(sha256.New, []byte(env.secretKey))
		_, err = h.Write(body)
		require.NoError(t, err)

		sig := h.Sum(nil)
		assert.True(t, hmac.Equal(sig, agoraSignature))

		assert.EqualValues(t, expectedReq, body)

		resp.WriteHeader(statusCode)
		resp.Header().Set("Content-Type", "application/json")
		_, err = resp.Write(respBody)
		require.NoError(t, err)
	}))
	return testServer
}
