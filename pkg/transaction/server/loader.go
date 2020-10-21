package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"net/http"

	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/go/clients/horizon"
	"github.com/kinecosystem/go/xdr"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	transactionpb "github.com/kinecosystem/agora-api/genproto/transaction/v3"

	"github.com/kinecosystem/agora/pkg/transaction/history"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

type txData struct {
	ledger      int64
	hash        []byte
	envelopeXDR []byte
	resultXDR   []byte

	cursor   *transactionpb.Cursor
	memo     *kin.Memo
	textMemo string
}

type historyLoader struct {
	log    *logrus.Entry
	client horizon.ClientInterface

	reader    history.Reader
	committer ingestion.Committer
}

func newLoader(client horizon.ClientInterface, reader history.Reader, committer ingestion.Committer) *historyLoader {
	return &historyLoader{
		log:    logrus.StandardLogger().WithField("type", "transaction/server/historyLoader"),
		client: client,

		reader:    reader,
		committer: committer,
	}
}

func (h *historyLoader) getTransaction(ctx context.Context, hash []byte) (txn txData, err error) {
	log := h.log.WithFields(logrus.Fields{
		"method": "getTransaction",
		"hash":   hash,
	})

	entry, err := h.reader.GetTransaction(ctx, hash)
	if err == nil {
		txn, err = txDataFromEntry(entry)
		if err == nil {
			return txn, nil
		}
	}

	log.WithError(err).Warn("Failed to get transaction entry, falling back")

	tx, err := h.client.LoadTransaction(hex.EncodeToString(hash))
	if err != nil {
		if hErr, ok := err.(*horizon.Error); ok {
			switch hErr.Problem.Status {
			case http.StatusNotFound:
				return txn, history.ErrNotFound
			}
		}

		return txn, errors.Wrap(err, "unexpected error from horizon")
	}

	return txDataFromHorizon(tx)
}

func (h *historyLoader) getTransactions(ctx context.Context, account string, cursor *transactionpb.Cursor, order transactionpb.GetHistoryRequest_Direction) ([]txData, error) {
	log := h.log.WithFields(logrus.Fields{
		"method":  "getTransactions",
		"account": account,
	})

	opts := &history.ReadOptions{
		Limit:      100,
		Descending: order == transactionpb.GetHistoryRequest_DESC,
	}
	if cursor != nil {
		start, err := startFromCursor(cursor)
		if err != nil {
			log.WithError(err).Warn("failed to get start from cursor, ignoring")
		} else {
			opts.Start = start
		}
	} else if opts.Descending {
		var latestEntry *model.Entry
		latestEntry, err := h.reader.GetLatestForAccount(ctx, account)
		if err == history.ErrNotFound {
			return nil, nil
		} else if err != nil {
			return nil, errors.Wrap(err, "failed to get last entry for account")
		}

		opts.Start, err = latestEntry.GetOrderingKey()
		if err != nil {
			return nil, errors.Wrap(err, "failed to get ordering key from latest entry")
		}
	}

	entries, err := h.reader.GetAccountTransactions(ctx, account, opts)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get account transactions")
	}
	if len(entries) == 0 {
		return nil, nil
	}

	latestVersion := entries[len(entries)-1].Version
	latestCommit, err := h.committer.Latest(ctx, ingestion.GetHistoryIngestorName(latestVersion))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get latest pointer for %s", latestVersion.String())
	}

	var txns []txData
	for _, e := range entries {
		orderingKey, err := e.GetOrderingKey()
		if err != nil {
			return nil, errors.Wrap(err, "failed to get ordering key")
		}

		// Filter out any entries that have not yet been marked as committed
		if bytes.Compare(orderingKey, latestCommit) > 0 {
			continue
		}

		// If a cursor was specified, we don't want to include that entry
		// in the results, as the caller presumably has it.
		//
		// Note: we do it here instead of the reader layer, as there may be
		//       cases where we want the start to be included (and notably, it's
		//       more natural to have the start be inclusive).
		if cursor != nil && bytes.Equal(opts.GetStart(), orderingKey) {
			continue
		}

		data, err := txDataFromEntry(e)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get txData from entry")
		}

		txns = append(txns, data)
	}

	return txns, nil
}

func txDataFromEntry(entry *model.Entry) (data txData, err error) {
	e, ok := entry.Kind.(*model.Entry_Stellar)
	if !ok {
		return data, errors.New("unsupported entry type")
	}

	orderingKey, err := entry.GetOrderingKey()
	if err != nil {
		// note: this should only fail if the type is unsupported.
		return data, errors.New("failed to get ordering key")
	}

	txID, err := entry.GetTxID()
	if err != nil {
		return data, errors.Wrap(err, "failed to get tx hash")
	}

	data.hash = txID
	data.ledger = int64(e.Stellar.Ledger)
	data.envelopeXDR = e.Stellar.EnvelopeXdr
	data.resultXDR = e.Stellar.ResultXdr
	data.cursor = &transactionpb.Cursor{
		Value: orderingKey,
	}

	var envelope xdr.TransactionEnvelope
	if err := envelope.UnmarshalBinary(data.envelopeXDR); err != nil {
		return data, errors.Wrap(err, "failed to unmarshal envelope bytes")
	}

	if envelope.Tx.Memo.Hash != nil {
		memo := kin.Memo(*envelope.Tx.Memo.Hash)
		if kin.IsValidMemoStrict(memo) {
			data.memo = &memo
		}
	} else if envelope.Tx.Memo.Text != nil {
		data.textMemo = *envelope.Tx.Memo.Text
	}

	return data, nil
}

func txDataFromHorizon(tx horizon.Transaction) (data txData, err error) {
	data.ledger = int64(tx.Ledger)

	data.hash, err = hex.DecodeString(tx.Hash)
	if err != nil {
		return data, errors.Wrap(err, "failed to decode hash")
	}
	data.envelopeXDR, err = base64.StdEncoding.DecodeString(tx.EnvelopeXdr)
	if err != nil {
		return data, errors.Wrap(err, "failed to decode envelope xdr")
	}
	data.resultXDR, err = base64.StdEncoding.DecodeString(tx.ResultXdr)
	if err != nil {
		return data, errors.Wrap(err, "failed to decode result xdr")
	}

	// todo(kin2,kin4): need context as to which blockchain we loaded from.
	orderKey, err := model.OrderingKeyFromCursor(model.KinVersion_KIN3, tx.PT)
	if err != nil {
		return data, errors.Wrap(err, "failed to get order key from cursor")
	}
	data.cursor = &transactionpb.Cursor{
		Value: orderKey,
	}

	if len(tx.Memo) > 0 {
		b, err := base64.StdEncoding.DecodeString(tx.Memo)
		if err != nil {
			return data, errors.Wrap(err, "failed to decode transaction memo")
		}

		var xm xdr.Memo
		err = xm.UnmarshalBinary(b)
		if err != nil {
			return data, errors.Wrap(err, "failed to unmarshal binary memo")
		}

		if memo, ok := kin.MemoFromXDR(xm, true); ok {
			data.memo = &memo
		} else if xm.Text != nil {
			data.textMemo = *xm.Text
		}
	}

	return data, nil
}

func startFromCursor(c *transactionpb.Cursor) ([]byte, error) {
	if c == nil || len(c.Value) == 0 {
		return nil, nil
	}

	// Horizon cursors are all strings representing an int64.
	// Agora cursors, on the other hand, are a (version, ordering_key) pair.
	// We can therefore differentiate the two by checking the first byte.
	//
	// If it's in the 0-9 ascii range (48 -> 57), then we assume it's a horizon
	// cursor. Otherwise, we can assume it's an agora cursor. We use a slightly
	// more aggressive range check to be sure.
	if c.Value[0] < 10 {
		return c.Value, nil
	}

	// todo(kin2,kin4): need context as to which blockchain we loaded from.
	orderKey, err := model.OrderingKeyFromCursor(model.KinVersion_KIN3, string(c.Value))
	if err != nil {
		return nil, errors.Wrap(err, "failed to get ordering key from transactionpb.Cursor")
	}

	return orderKey, nil
}
