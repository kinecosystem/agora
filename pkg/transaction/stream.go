package transaction

import (
	"bytes"
	"context"
	"encoding/base64"
	"time"

	"github.com/kinecosystem/agora-common/retry"
	"github.com/kinecosystem/agora-common/retry/backoff"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go/clients/horizonclient"
	hProtocol "github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/xdr"
)

type XDRData struct {
	Envelope xdr.TransactionEnvelope
	Result   xdr.TransactionResult
	Meta     xdr.TransactionMeta
}

// Notifier notifies that a new transaction has been confirmed on the blockchain.
type Notifier interface {
	OnTransaction(XDRData)
}

// StreamTransactions streams transactions from horizon, notifying the provided notifiers with received transactions.
func StreamTransactions(ctx context.Context, hClient horizonclient.ClientInterface, notifiers ...Notifier) {
	log := logrus.StandardLogger().WithFields(logrus.Fields{
		"type":   "transaction/stream",
		"method": "StreamTransactions",
	})
	req := horizonclient.TransactionRequest{
		Order:         horizonclient.OrderAsc,
		Cursor:        "now",
	}

	handler := func(t hProtocol.Transaction) {
		xdrData := XDRData{}

		envelopeBytes, err := base64.StdEncoding.DecodeString(t.EnvelopeXdr)
		if err != nil {
			log.WithError(err).Warn("failed to parse envelope XDR, dropping")
			return
		}

		if _, err := xdr.Unmarshal(bytes.NewBuffer(envelopeBytes), &xdrData.Envelope); err != nil {
			log.WithError(err).Warn("failed to unmarshal transaction envelope, dropping")
			return
		}

		resultBytes, err := base64.StdEncoding.DecodeString(t.ResultXdr)
		if err != nil {
			log.WithError(err).Warn("failed to parse result XDR, dropping")
			return
		}

		if _, err := xdr.Unmarshal(bytes.NewBuffer(resultBytes), &xdrData.Result); err != nil {
			log.WithError(err).Warn("failed to unmarshal transaction result, dropping")
			return
		}

		metaBytes, err := base64.StdEncoding.DecodeString(t.ResultMetaXdr)
		if err != nil {
			log.WithError(err).Warn("failed to parse result meta XDR, dropping")
			return
		}

		if _, err := xdr.Unmarshal(bytes.NewBuffer(metaBytes), &xdrData.Meta); err != nil {
			log.WithError(err).Warn("failed to unmarshal transaction meta, dropping")
			return
		}

		for _, n := range notifiers {
			n.OnTransaction(xdrData)
		}
	}

	var errContextCancelled error
	_, _ = retry.Retry(
		func() error {
			select {
			case <-ctx.Done():
				return errContextCancelled
			default:
			}

			// A nil error will only get returned if the context gets cancelled
			err := hClient.StreamTransactions(ctx, req, handler)
			if err != nil {
				log.WithError(err).Warn("failed to stream transactions")
			}
			return err
		},
		retry.NonRetriableErrors(errContextCancelled),
		retry.BackoffWithJitter(backoff.Constant(10*time.Second), 12*time.Second, 0.2),
	)
}
