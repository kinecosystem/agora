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

// Notifier notifies that a new transaction has been confirmed on the blockchain.
type Notifier interface {
	// NewTransaction
	NewTransaction(xdr.TransactionEnvelope, xdr.TransactionMeta)
}

// StreamTransactions streams transactions from horizon, notifying the provided notifiers with received transactions.
func StreamTransactions(ctx context.Context, hClient horizonclient.ClientInterface, notifiers ...Notifier) {
	log := logrus.StandardLogger().WithField("method", "StreamTransactions")
	req := horizonclient.TransactionRequest{
		Order:  horizonclient.OrderAsc,
		Cursor: "now",
	}

	handler := func(t hProtocol.Transaction) {
		envelopeBytes, err := base64.StdEncoding.DecodeString(t.EnvelopeXdr)
		if err != nil {
			log.WithError(err).Warn("failed to parse envelope XDR")
		}

		var e xdr.TransactionEnvelope
		if _, err := xdr.Unmarshal(bytes.NewBuffer(envelopeBytes), &e); err != nil {
			log.WithError(err).Warn("failed to unmarshal transaction envelope")
		}

		metaBytes, err := base64.StdEncoding.DecodeString(t.ResultMetaXdr)
		if err != nil {
			log.WithError(err).Warn("failed to parse result meta XDR")
		}

		var m xdr.TransactionMeta
		if _, err := xdr.Unmarshal(bytes.NewBuffer(metaBytes), &m); err != nil {
			log.WithError(err).Warn("failed to unmarshal transaction meta")
		}

		for _, n := range notifiers {
			n.NewTransaction(e, m)
		}
	}

	_, _ = retry.Retry(
		func() error {
			// A nil error will only get returned if the context gets cancelled
			err := hClient.StreamTransactions(ctx, req, handler)
			if err != nil {
				log.WithError(err).Warn("failed to stream transactions")
			}
			return err
		},
		retry.BackoffWithJitter(backoff.Constant(10*time.Second), 12*time.Second, 0.2),
	)
}
