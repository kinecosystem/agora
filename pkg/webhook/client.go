package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/kinecosystem/agora-common/headers"
	"github.com/kinecosystem/agora-common/retry"
	"github.com/kinecosystem/agora-common/retry/backoff"
	"github.com/kinecosystem/go/xdr"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/kinecosystem/agora/pkg/app"
	"github.com/kinecosystem/agora/pkg/webhook/signtransaction"
)

const (
	AppUserIDCtxHeader      = "app-user-id"
	AppUserPasskeyCtxHeader = "app-user-passkey"

	AgoraHMACHeader      = "X-Agora-HMAC-SHA256"
	AppUserIDHeader      = "X-App-User-ID"
	AppUserPasskeyHeader = "X-App-User-Passkey"
)

type Client struct {
	log        *logrus.Entry
	httpClient *http.Client
}

type SignTransactionError struct {
	Message         string
	StatusCode      int
	OperationErrors []signtransaction.InvoiceError
}

func (e *SignTransactionError) Error() string {
	return fmt.Sprintf("%s (status code: %d)", e.Message, e.StatusCode)
}

// NewClient returns a client which can be used to submit requests to app webhooks.
func NewClient(httpClient *http.Client) *Client {
	return &Client{
		log:        logrus.StandardLogger().WithField("type", "webhook/client"),
		httpClient: httpClient,
	}
}

// SignTransaction submits a sign transaction request to an app webhook
func (c *Client) SignTransaction(ctx context.Context, signURL url.URL, webhookSecret []byte, req *signtransaction.RequestBody) (encodedXDR string, envelopeXDR *xdr.TransactionEnvelope, err error) {
	signTxJSON, err := json.Marshal(req)
	if err != nil {
		return "", nil, errors.Wrap(err, "failed to marshal sign transaction request body")
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, signURL.String(), bytes.NewBuffer(signTxJSON))
	if err != nil {
		return "", nil, errors.Wrap(err, "failed to create sign transaction http request")
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if err := sign(httpReq, webhookSecret, signTxJSON); err != nil {
		return "", nil, err
	}

	userID, err := headers.GetASCIIHeaderByName(ctx, AppUserIDCtxHeader)
	if err != nil {
		return "", nil, errors.Wrap(err, "failed to get app user ID header")
	}
	userPasskey, err := headers.GetASCIIHeaderByName(ctx, AppUserPasskeyCtxHeader)
	if err != nil {
		return "", nil, errors.Wrap(err, "failed to get app user passkey header")
	}

	if (userID != "") != (userPasskey != "") {
		return "", nil, errors.New("if app user auth headers are used, both must be present")
	} else if userID != "" {
		httpReq.Header.Set(AppUserIDHeader, userID)
		httpReq.Header.Set(AppUserPasskeyHeader, userPasskey)
	}

	var resp *http.Response
	_, err = retry.Retry(
		func() error {
			resp, err = c.httpClient.Do(httpReq)
			return err
		},
		retry.Limit(3),
		retry.BackoffWithJitter(backoff.BinaryExponential(100*time.Millisecond), 440*time.Millisecond, 0.1),
		retry.NonRetriableErrors(app.ErrURLNotSet),
	)
	defer resp.Body.Close()
	if err != nil {
		return "", nil, errors.Wrap(err, "failed to call sign transaction webhook")
	}

	if resp.StatusCode == 200 {
		decodedResp := &signtransaction.SuccessResponse{}
		err = json.NewDecoder(resp.Body).Decode(decodedResp)
		if err != nil {
			return "", nil, errors.Wrap(err, "failed to decode 200 response")
		}
		e, err := decodedResp.GetEnvelopeXDR()
		if err != nil {
			return "", nil, errors.Wrap(err, "received invalid response")
		}

		return decodedResp.EnvelopeXDR, e, nil
	}

	if resp.StatusCode == 400 {
		decodedResp := &signtransaction.BadRequestResponse{}
		err := json.NewDecoder(resp.Body).Decode(decodedResp)
		if err != nil {
			return "", nil, errors.Wrap(err, "failed to decode 400 response")
		}

		return "", nil, &SignTransactionError{Message: decodedResp.Message, StatusCode: 400}
	}

	if resp.StatusCode == 403 {
		decodedResp := &signtransaction.ForbiddenResponse{}
		err := json.NewDecoder(resp.Body).Decode(decodedResp)
		if err != nil {
			return "", nil, errors.Wrap(err, "failed to decode 403 response")
		}

		return "", nil, &SignTransactionError{Message: decodedResp.Message, StatusCode: 403, OperationErrors: decodedResp.InvoiceErrors}
	}

	return "", nil, &SignTransactionError{Message: "failed to sign transaction", StatusCode: resp.StatusCode}
}

func (c *Client) Events(ctx context.Context, eventsURL url.URL, webhookSecret, body []byte) error {
	log := c.log.WithFields(logrus.Fields{
		"method": "Events",
		"url":    eventsURL.String(),
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, eventsURL.String(), bytes.NewBuffer(body))
	if err != nil {
		return errors.Wrap(err, "failed to create sign transaction http request")
	}
	req.Header.Set("Content-Type", "application/json")
	if err := sign(req, webhookSecret, body); err != nil {
		return err
	}

	var resp *http.Response
	_, err = retry.Retry(
		func() error {
			resp, err = c.httpClient.Do(req)
			return err
		},
		retry.Limit(5),
		retry.BackoffWithJitter(backoff.BinaryExponential(100*time.Millisecond), 5*time.Second, 0.1),
	)
	if err != nil {
		return errors.Wrap(err, "failed to call sign transaction webhook")
	}
	defer resp.Body.Close()

	// Anything that's not a 500 is likely a misconfigured webhook.
	// Since we're not guaranteeing that all transactions will be delivered,
	// we just mark it as OK so the events processor can make progress for this
	// endpoint.
	if resp.StatusCode < 500 {
		if resp.StatusCode >= 300 {
			log.WithFields(logrus.Fields{
				"code":   resp.StatusCode,
				"status": resp.Status,
			}).Info("Non-retriable error code, ignoring")
		}

		return nil
	}

	return errors.Errorf("webhook error: %d", resp.StatusCode)
}

func sign(req *http.Request, secret, body []byte) error {
	if len(secret) < 32 {
		return errors.New("webhook secret must be at least 32 bytes")
	}

	h := hmac.New(sha256.New, secret)
	_, err := h.Write(body)
	if err != nil {
		return errors.Wrap(err, "failed to generate hmac signature")
	}

	req.Header.Set(AgoraHMACHeader, base64.StdEncoding.EncodeToString(h.Sum(nil)))
	return nil
}
