package client

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/agora-common/kin"
	"github.com/pkg/errors"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"

	"github.com/kinecosystem/agora/pkg/testutil"
	"github.com/kinecosystem/agora/pkg/webhook"
	"github.com/kinecosystem/agora/pkg/webhook/events"
	"github.com/kinecosystem/agora/pkg/webhook/signtransaction"
)

func TestEventsHandler(t *testing.T) {
	data := []events.Event{
		{
			TransactionEvent: &events.TransactionEvent{
				KinVersion: 1,
				TxHash:     []byte("hash"),
				InvoiceList: &commonpb.InvoiceList{
					Invoices: []*commonpb.Invoice{
						{
							Items: []*commonpb.Invoice_LineItem{
								{
									Title: "hello",
								},
							},
						},
					},
				},
				StellarEvent: &events.StellarData{
					EnvelopeXDR: []byte("envelope"),
					ResultXDR:   []byte("result"),
				},
			},
		},
	}

	called := false
	f := func(events []events.Event) error {
		called = true
		assert.Equal(t, data, events)
		return nil
	}

	body, err := json.Marshal(data)
	require.NoError(t, err)

	secret := "secret"
	b := bytes.NewBuffer(body)
	h := hmac.New(sha256.New, []byte(secret))
	_, _ = h.Write(b.Bytes())
	sig := h.Sum(nil)

	req, err := http.NewRequest(http.MethodPost, "/events", b)
	require.NoError(t, err)
	req.Header.Add(webhook.AgoraHMACHeader, base64.StdEncoding.EncodeToString(sig[:]))

	rr := httptest.NewRecorder()
	handler := EventsHandler(secret, f)
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.True(t, called)

	// if no webhook secret was provided, don't validate
	req, err = http.NewRequest(http.MethodPost, "/events", bytes.NewBuffer(body))
	require.NoError(t, err)
	req.Header.Add(webhook.AgoraHMACHeader, base64.StdEncoding.EncodeToString([]byte("fake sig")))

	rr = httptest.NewRecorder()
	handler = EventsHandler("", f)
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.True(t, called)

	f = func([]events.Event) error {
		return errors.New("server error")
	}

	b = bytes.NewBuffer(body)

	req, err = http.NewRequest(http.MethodPost, "/events", b)
	require.NoError(t, err)
	req.Header.Add(webhook.AgoraHMACHeader, base64.StdEncoding.EncodeToString(sig[:]))

	rr = httptest.NewRecorder()
	handler = EventsHandler(secret, f)
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
}

func TestEventsHandler_Invalid(t *testing.T) {
	f := func(events []events.Event) error {
		t.Fail()
		return nil
	}

	var invalidMethodRequest []*http.Request
	invalidMethods := []string{
		http.MethodConnect,
		http.MethodDelete,
		http.MethodGet,
		http.MethodPatch,
		http.MethodPut,
		http.MethodTrace,
	}
	for _, m := range invalidMethods {
		req, err := http.NewRequest(m, "/events", nil)
		require.NoError(t, err)
		invalidMethodRequest = append(invalidMethodRequest, req)
	}
	for _, r := range invalidMethodRequest {
		rr := httptest.NewRecorder()
		handler := EventsHandler("secret", f)
		handler.ServeHTTP(rr, r)

		assert.Equal(t, http.StatusMethodNotAllowed, rr.Code)
	}

	secret := "secret"
	b := bytes.NewBuffer([]byte("{"))
	h := hmac.New(sha256.New, []byte(secret))
	_, _ = h.Write(b.Bytes())
	sig := h.Sum(nil)

	// Generic bad request
	req, err := http.NewRequest(http.MethodPost, "/events", b)
	require.NoError(t, err)
	req.Header.Add(webhook.AgoraHMACHeader, base64.StdEncoding.EncodeToString(sig[:]))

	rr := httptest.NewRecorder()
	handler := EventsHandler("secret", f)
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)

	// Invalid sig
	req, err = http.NewRequest(http.MethodPost, "/events", b)
	require.NoError(t, err)
	req.Header.Add(webhook.AgoraHMACHeader, base64.StdEncoding.EncodeToString([]byte("fake sig")))

	rr = httptest.NewRecorder()
	handler = EventsHandler("secret", f)
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)

	// No sig
	req, err = http.NewRequest(http.MethodPost, "/events", b)
	require.NoError(t, err)

	rr = httptest.NewRecorder()
	handler = EventsHandler("secret", f)
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestSignTransactionHandler(t *testing.T) {
	whitelist, err := NewPrivateKey()
	require.NoError(t, err)

	called := false
	f := func(req SignTransactionRequest, resp *SignTransactionResponse) error {
		assert.Len(t, req.Envelope.Tx.Operations, 10)
		assert.Len(t, req.Payments, 10)

		var memoCount, invoiceCount int
		for _, p := range req.Payments {
			assert.NotEmpty(t, p.Sender)
			assert.NotEmpty(t, p.Destination)
			assert.NotZero(t, p.Quarks)
			assert.Equal(t, kin.TransactionTypeSpend, p.Type)

			if p.Memo != "" {
				memoCount++
			}
			if p.Invoice != nil {
				invoiceCount++
			}
		}

		if memoCount > 0 {
			assert.Equal(t, 10, memoCount)
			assert.Zero(t, invoiceCount)
		} else if invoiceCount > 0 {
			assert.Zero(t, memoCount)
			assert.Equal(t, 10, invoiceCount)
		} else {
			assert.Zero(t, memoCount)
			assert.Zero(t, invoiceCount)
		}

		called = true
		return resp.Sign(PrivateKey(whitelist))
	}

	signRequests := []signtransaction.RequestBody{
		genRequest(t, xdr.MemoTypeMemoNone),
		genRequest(t, xdr.MemoTypeMemoText),
		genRequest(t, xdr.MemoTypeMemoHash),
	}
	for _, data := range signRequests {
		body, err := json.Marshal(data)
		require.NoError(t, err)

		secret := "secret"
		b := bytes.NewBuffer(body)
		h := hmac.New(sha256.New, []byte(secret))
		_, _ = h.Write(b.Bytes())
		sig := h.Sum(nil)

		req, err := http.NewRequest(http.MethodPost, "/sign_transaction", b)
		require.NoError(t, err)
		req.Header.Add(webhook.AgoraHMACHeader, base64.StdEncoding.EncodeToString(sig[:]))

		rr := httptest.NewRecorder()
		handler := SignTransactionHandler(EnvironmentTest, secret, f)
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		assert.True(t, called)
		called = false

		var resp signtransaction.SuccessResponse
		assert.NoError(t, json.NewDecoder(rr.Result().Body).Decode(&resp))

		var envelope xdr.TransactionEnvelope
		assert.NoError(t, envelope.UnmarshalBinary(resp.EnvelopeXDR))
		assert.Len(t, envelope.Signatures, 1)
	}

	// if no webhook secret was provided, don't validate
	signRequest := genRequest(t, xdr.MemoTypeMemoNone)
	body, err := json.Marshal(signRequest)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, "/sign_transaction", bytes.NewBuffer(body))
	require.NoError(t, err)
	req.Header.Add(webhook.AgoraHMACHeader, base64.StdEncoding.EncodeToString([]byte("fake sig")))

	rr := httptest.NewRecorder()
	handler := SignTransactionHandler(EnvironmentTest, "", f)
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.True(t, called)
}

func TestSignTransactionHandler_Rejected(t *testing.T) {
	called := false
	f := func(req SignTransactionRequest, resp *SignTransactionResponse) error {
		called = true
		resp.Reject()
		return nil
	}

	signRequests := []signtransaction.RequestBody{
		genRequest(t, xdr.MemoTypeMemoNone),
		genRequest(t, xdr.MemoTypeMemoText),
		genRequest(t, xdr.MemoTypeMemoHash),
	}
	for _, data := range signRequests {
		body, err := json.Marshal(data)
		require.NoError(t, err)

		secret := "secret"
		b := bytes.NewBuffer(body)
		h := hmac.New(sha256.New, []byte(secret))
		_, _ = h.Write(b.Bytes())
		sig := h.Sum(nil)

		req, err := http.NewRequest(http.MethodPost, "/sign_transaction", b)
		require.NoError(t, err)
		req.Header.Add(webhook.AgoraHMACHeader, base64.StdEncoding.EncodeToString(sig[:]))

		rr := httptest.NewRecorder()
		handler := SignTransactionHandler(EnvironmentTest, secret, f)
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusForbidden, rr.Code)
		assert.True(t, called)
		called = false

		var resp signtransaction.ForbiddenResponse
		assert.NoError(t, json.NewDecoder(rr.Result().Body).Decode(&resp))
		assert.Equal(t, resp.Message, "rejected")
		assert.Empty(t, resp.InvoiceErrors)
	}
}

func TestSignTransactionHandler_InvoiceErrors(t *testing.T) {
	called := false
	f := func(req SignTransactionRequest, resp *SignTransactionResponse) error {
		called = true

		resp.MarkAlreadyPaid(0)
		resp.MarkWrongDestination(3)
		resp.MarkSKUNotFound(5)

		return nil
	}

	signRequests := []signtransaction.RequestBody{
		genRequest(t, xdr.MemoTypeMemoNone),
		genRequest(t, xdr.MemoTypeMemoText),
		genRequest(t, xdr.MemoTypeMemoHash),
	}
	for _, data := range signRequests {
		body, err := json.Marshal(data)
		require.NoError(t, err)

		secret := "secret"
		b := bytes.NewBuffer(body)
		h := hmac.New(sha256.New, []byte(secret))
		_, _ = h.Write(b.Bytes())
		sig := h.Sum(nil)

		req, err := http.NewRequest(http.MethodPost, "/sign_transaction", b)
		require.NoError(t, err)
		req.Header.Add(webhook.AgoraHMACHeader, base64.StdEncoding.EncodeToString(sig[:]))

		rr := httptest.NewRecorder()
		handler := SignTransactionHandler(EnvironmentTest, secret, f)
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusForbidden, rr.Code)
		assert.True(t, called)
		called = false

		var resp signtransaction.ForbiddenResponse
		assert.NoError(t, json.NewDecoder(rr.Result().Body).Decode(&resp))
		assert.Equal(t, resp.Message, "rejected")
		assert.Len(t, resp.InvoiceErrors, 3)

		assert.EqualValues(t, resp.InvoiceErrors[0].OperationIndex, 0)
		assert.EqualValues(t, resp.InvoiceErrors[0].Reason, signtransaction.AlreadyPaid)
		assert.EqualValues(t, resp.InvoiceErrors[1].OperationIndex, 3)
		assert.EqualValues(t, resp.InvoiceErrors[1].Reason, signtransaction.WrongDestination)
		assert.EqualValues(t, resp.InvoiceErrors[2].OperationIndex, 5)
		assert.EqualValues(t, resp.InvoiceErrors[2].Reason, signtransaction.SKUNotFound)
	}
}
func TestSignTransactionHandler_Invalid(t *testing.T) {
	f := func(req SignTransactionRequest, resp *SignTransactionResponse) error {
		t.Fail()
		return nil
	}

	var invalidMethodRequest []*http.Request
	invalidMethods := []string{
		http.MethodConnect,
		http.MethodDelete,
		http.MethodGet,
		http.MethodPatch,
		http.MethodPut,
		http.MethodTrace,
	}
	for _, m := range invalidMethods {
		req, err := http.NewRequest(m, "/sign_transaction", nil)
		require.NoError(t, err)
		invalidMethodRequest = append(invalidMethodRequest, req)
	}
	for _, r := range invalidMethodRequest {
		rr := httptest.NewRecorder()
		handler := SignTransactionHandler(EnvironmentTest, "secret", f)
		handler.ServeHTTP(rr, r)

		assert.Equal(t, http.StatusMethodNotAllowed, rr.Code)
	}

	secret := "secret"
	b := bytes.NewBuffer([]byte("{"))
	h := hmac.New(sha256.New, []byte(secret))
	_, _ = h.Write(b.Bytes())
	sig := h.Sum(nil)

	// Generic bad request
	req, err := http.NewRequest(http.MethodPost, "/sign_transaction", b)
	require.NoError(t, err)
	req.Header.Add(webhook.AgoraHMACHeader, base64.StdEncoding.EncodeToString(sig[:]))

	rr := httptest.NewRecorder()
	handler := SignTransactionHandler(EnvironmentTest, "secret", f)
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)

	// Invalid sig
	req, err = http.NewRequest(http.MethodPost, "/sign_transaction", b)
	require.NoError(t, err)
	req.Header.Add(webhook.AgoraHMACHeader, base64.StdEncoding.EncodeToString([]byte("fake sig")))

	rr = httptest.NewRecorder()
	handler = SignTransactionHandler(EnvironmentTest, "secret", f)
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)

	// No sig
	req, err = http.NewRequest(http.MethodPost, "/sign_transaction", b)
	require.NoError(t, err)

	rr = httptest.NewRecorder()
	handler = SignTransactionHandler(EnvironmentTest, "secret", f)
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)

	makeReq := func(r signtransaction.RequestBody) *http.Request {
		body, err := json.Marshal(&r)
		require.NoError(t, err)

		b := bytes.NewBuffer(body)
		h := hmac.New(sha256.New, []byte(secret))
		_, _ = h.Write(b.Bytes())
		sig := h.Sum(nil)

		req, err = http.NewRequest(http.MethodPost, "/sign_transaction", b)
		require.NoError(t, err)
		req.Header.Add(webhook.AgoraHMACHeader, base64.StdEncoding.EncodeToString(sig[:]))
		return req
	}

	// Generate a request with mis-matched invoice counts
	signReq := genRequest(t, xdr.MemoTypeMemoHash)
	invoiceList := &commonpb.InvoiceList{}
	assert.NoError(t, proto.Unmarshal(signReq.InvoiceList, invoiceList))
	invoiceList.Invoices = invoiceList.Invoices[1:]
	ilBytes, err := proto.Marshal(invoiceList)
	require.NoError(t, err)
	signReq.InvoiceList = ilBytes

	rr = httptest.NewRecorder()
	handler = SignTransactionHandler(EnvironmentTest, "secret", f)
	handler.ServeHTTP(rr, makeReq(signReq))

	assert.Equal(t, http.StatusBadRequest, rr.Code)

	// Generate a request with malformed XDR
	signReq = genRequest(t, xdr.MemoTypeMemoHash)
	signReq.EnvelopeXDR = signReq.EnvelopeXDR[1:]

	rr = httptest.NewRecorder()
	handler = SignTransactionHandler(EnvironmentTest, "secret", f)
	handler.ServeHTTP(rr, makeReq(signReq))
	assert.Equal(t, http.StatusBadRequest, rr.Code)

	// Gnerate a request with a malformed invoice list
	signReq = genRequest(t, xdr.MemoTypeMemoHash)
	signReq.InvoiceList = signReq.InvoiceList[1:]
	rr = httptest.NewRecorder()
	handler = SignTransactionHandler(EnvironmentTest, "secret", f)
	handler.ServeHTTP(rr, makeReq(signReq))
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func genRequest(t *testing.T, memoType xdr.MemoType) signtransaction.RequestBody {
	accounts := testutil.GenerateAccountIDs(t, 10)
	invoiceList := &commonpb.InvoiceList{}

	var ops []xdr.Operation
	for i := 0; i < 10; i++ {
		ops = append(ops, testutil.GeneratePaymentOperation(&accounts[0], accounts[i]))
		invoiceList.Invoices = append(invoiceList.Invoices, &commonpb.Invoice{
			Items: []*commonpb.Invoice_LineItem{
				{
					Title:  "test",
					Amount: int64(i),
				},
			},
		})
	}

	envelope := testutil.GenerateTransactionEnvelope(accounts[0], 1, ops)
	req := signtransaction.RequestBody{}

	switch memoType {
	case xdr.MemoTypeMemoText:
		m := "1-test"
		envelope.Tx.Memo = xdr.Memo{
			Type: memoType,
			Text: &m,
		}
	case xdr.MemoTypeMemoHash:
		ilBytes, err := proto.Marshal(invoiceList)
		require.NoError(t, err)
		req.InvoiceList = ilBytes

		var placeholder xdr.Hash
		envelope.Tx.Memo = xdr.Memo{
			Type: memoType,
			Hash: &placeholder,
		}
	}

	var err error
	req.EnvelopeXDR, err = envelope.MarshalBinary()
	require.NoError(t, err)
	return req
}
