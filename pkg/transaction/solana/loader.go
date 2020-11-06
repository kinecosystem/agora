package solana

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"math"
	"sort"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/kinecosystem/go/strkey"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go/xdr"
	"golang.org/x/sync/errgroup"

	commonpbv3 "github.com/kinecosystem/agora-api/genproto/common/v3"
	commonpbv4 "github.com/kinecosystem/agora-api/genproto/common/v4"
	transactionpb "github.com/kinecosystem/agora-api/genproto/transaction/v4"

	"github.com/kinecosystem/agora/pkg/invoice"
	"github.com/kinecosystem/agora/pkg/solanautil"
	"github.com/kinecosystem/agora/pkg/transaction/history"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

type loader struct {
	log          *logrus.Entry
	sc           solana.Client
	rw           history.ReaderWriter
	committer    ingestion.Committer
	invoiceStore invoice.Store
	token        ed25519.PublicKey
}

func newLoader(
	sc solana.Client,
	rw history.ReaderWriter,
	committer ingestion.Committer,
	invoiceStore invoice.Store,
	token ed25519.PublicKey,
) *loader {
	return &loader{
		log:          logrus.StandardLogger().WithField("type", "transaction/solana/loader"),
		sc:           sc,
		rw:           rw,
		committer:    committer,
		invoiceStore: invoiceStore,
		token:        token,
	}
}

func (l *loader) loadTransaction(ctx context.Context, id []byte) (*transactionpb.GetTransactionResponse, error) {
	log := l.log.WithFields(logrus.Fields{
		"method": "loadTransaction",
		"id":     base64.StdEncoding.EncodeToString(id),
	})

	il, err := l.invoiceStore.Get(ctx, id)
	if err != nil && err != invoice.ErrNotFound {
		return nil, errors.Wrap(err, "failed to load invoice list")
	}

	if len(id) == 32 {
		return l.getStellarResponse(ctx, id, il)
	} else if len(id) != ed25519.SignatureSize {
		return nil, errors.New("invalid id size")
	}

	var sig solana.Signature
	copy(sig[:], id)

	var writeback bool

	entry, err := l.rw.GetTransaction(ctx, id)
	if err != nil && err != history.ErrNotFound {
		return nil, errors.Wrap(err, "failed to load history entry")
	} else if err == history.ErrNotFound {
		// If the entry isn't in history, then either:
		//
		//   1. we failed to ingest it correctly on Submit().
		//   2. it was submitted outside of our system.
		//   3. it doesn't exist.
		//
		// It is possible to reconstruct the entry if it's
		// confirmed, so we try and do so here.
		writeback = true

		txn, err := l.sc.GetConfirmedTransaction(sig)
		if err == solana.ErrSignatureNotFound {
			return &transactionpb.GetTransactionResponse{
				State: transactionpb.GetTransactionResponse_UNKNOWN,
			}, nil
		}
		if err != nil {
			return nil, errors.Wrap(err, "failed to check confirmed transactions")
		}

		var rawTxError []byte
		if txn.Err != nil {
			str, err := txn.Err.JSONString()
			if err != nil {
				return nil, errors.Wrap(err, "failed to marshal raw transaction error")
			}

			rawTxError = []byte(str)
		}

		entry = &model.Entry{
			Version: 4,
			Kind: &model.Entry_Solana{
				Solana: &model.SolanaEntry{
					Slot:             txn.Slot,
					Confirmed:        true,
					Transaction:      txn.Transaction.Marshal(),
					TransactionError: rawTxError,
				},
			},
		}
	}

	var confirmations int
	if !entry.GetSolana().Confirmed {
		// todo: should we allow commitment specification here? it's not super important.
		stat, err := l.sc.GetSignatureStatus(sig, solana.CommitmentRecent)
		if err != nil && err != solana.ErrSignatureNotFound {
			return nil, errors.Wrap(err, "failed to query signature status")
		}

		entry.GetSolana().Slot = stat.Slot

		// no signature status simply means there is no entry in Solana's
		// signature status cache. This either means the transaction does not exist,
		// or is old enough to be evicted. Since we already have an entry, we know it's
		// the latter.
		//
		// Alternatively, if we have the status, and confirmations isn't set, it
		// means the transaction was rooted, and therefore confirmed.
		if err == solana.ErrSignatureNotFound || stat.Confirmations == nil {
			writeback = true
			entry.GetSolana().Confirmed = true
		} else if stat.Confirmations != nil {
			confirmations = *stat.Confirmations
		}
	}

	if writeback {
		// todo: should we do this asynchronously? should be fairly rare case.
		if err := l.rw.Write(context.Background(), entry); err != nil {
			log.WithError(err).Warn("failed to writeback transaction")
		}
	}

	item, err := historyItemFromEntry(entry)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get history item from entry")
	}
	item.InvoiceList = il

	resp := &transactionpb.GetTransactionResponse{
		Slot: entry.GetSolana().Slot,
		Item: item,
	}
	if len(entry.GetSolana().TransactionError) > 0 {
		resp.State = transactionpb.GetTransactionResponse_FAILED
	} else if !entry.GetSolana().Confirmed {
		resp.State = transactionpb.GetTransactionResponse_PENDING
		resp.Confirmations = uint32(confirmations)
	} else {
		resp.State = transactionpb.GetTransactionResponse_SUCCESS
	}

	return resp, nil
}

func (l *loader) getItems(ctx context.Context, accountID []byte, cursor *transactionpb.Cursor, order transactionpb.GetHistoryRequest_Direction) ([]*transactionpb.HistoryItem, error) {
	log := l.log.WithField("method", "getTransactions")

	// Unfortunately, we stored the account keys based on stellar strings, so we need
	// to transform the accountID -> string representation.
	account, err := strkey.Encode(strkey.VersionByteAccountID, accountID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to encode account id")
	}
	log = log.WithField("account", account)

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
		latestEntry, err := l.rw.GetLatestForAccount(ctx, account)
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

	entries, err := l.getEntriesForAccount(ctx, accountID, opts)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get account transactions")
	}
	if len(entries) == 0 {
		return nil, nil
	}

	// It is possible that there are no commits yet for a given blockchain version. If this is the case,
	// Latest() will return nil, which will result in no history being returned. However, it is possible that
	// there are entries for older blockchain versions, that _do_ have commits. In that case, we should be filtering
	// based on those.
	var highestVersion model.KinVersion
	if opts.Descending {
		highestVersion = entries[0].Version
	} else {
		highestVersion = entries[len(entries)-1].Version
	}

	var latestCommit ingestion.Pointer
	for v := highestVersion; v >= model.KinVersion_KIN2; v-- {
		latestCommit, err = l.committer.Latest(ctx, ingestion.GetHistoryIngestorName(v))
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get latest pointer for %s", v.String())
		}
		if latestCommit != nil {
			break
		}
	}

	commitCheckRequired := true
	var items []*transactionpb.HistoryItem
	for _, e := range entries {
		orderingKey, err := e.GetOrderingKey()
		if err != nil {
			return nil, errors.Wrap(err, "failed to get ordering key")
		}

		// Filter out any entries that have not yet been marked as committed
		//
		// todo: if we have the correct iteration order, just break
		// todo: if we have the correct order, sort.Search it?
		if commitCheckRequired && bytes.Compare(orderingKey, latestCommit) > 0 {
			// If we're ascending, then we know that any subsequent entries
			// will be greater than the commit, so we can stop processing early.
			if !opts.Descending {
				break
			}

			// Otherwise, subsequent entries may start to be less than the
			// latest commit, so we must continue.
			//
			// We could establish the starting point better by using something
			// like sort.Search, which can find it in O(log n), but we don't
			// expect the entry set to be particularly large after limit(),
			// and we also expect that only a few entries will be non-committed.
			//
			// Therefore, we take a naive approach as to just mark when we no
			// longer need to do commit checks. If our assumptions above don't
			// hold true, we can always try a more sophisticated approach, but
			// to avoid _too_ much micro-optimizing, we hold off.
			continue
		} else if opts.Descending {
			commitCheckRequired = false
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

		id, err := e.GetTxID()
		if err != nil {
			return nil, errors.Wrap(err, "failed to get tx hash for entry")
		}

		il, err := l.invoiceStore.Get(ctx, id)
		if err != nil && err != invoice.ErrNotFound {
			return nil, errors.Wrap(err, "failed to get invoice list for entry")
		}

		item, err := historyItemFromEntry(e)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get history item from entry")
		}
		item.InvoiceList = il

		items = append(items, item)
	}

	return items, nil
}

func (l *loader) getStellarResponse(ctx context.Context, id []byte, invoiceList *commonpbv3.InvoiceList) (*transactionpb.GetTransactionResponse, error) {
	entry, err := l.rw.GetTransaction(ctx, id)
	if err == history.ErrNotFound {
		// todo: should we be querying horizon here? in theory this shouldn't
		//       be hit on live because you cannot go backwards. It gets a bit
		//       harry since we'd need to infer kin2/kin3.
		return &transactionpb.GetTransactionResponse{
			State: transactionpb.GetTransactionResponse_UNKNOWN,
		}, nil
	} else if err != nil {
		return nil, errors.Wrap(err, "failed to load transaction from history")
	}

	item, err := historyItemFromEntry(entry)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get history item from entry")
	}
	item.InvoiceList = invoiceList

	return &transactionpb.GetTransactionResponse{
		State: transactionpb.GetTransactionResponse_SUCCESS,
		Item:  item,
	}, nil
}

func (l *loader) getEntriesForAccount(ctx context.Context, owner []byte, opts *history.ReadOptions) ([]*model.Entry, error) {
	addresses, err := l.sc.GetTokenAccountsByOwner(owner, l.token)
	if err != nil {
		return nil, errors.Wrap(err, "failed to resolve token accounts")
	}
	addresses = append(addresses, owner)

	var mu sync.Mutex
	var entries []*model.Entry

	// todo(scaling): bound for large amounts of token owners
	var group errgroup.Group
	for i := 0; i < len(addresses); i++ {
		account, err := strkey.Encode(strkey.VersionByteAccountID, addresses[i])
		if err != nil {
			return nil, errors.Wrap(err, "failed to encode account id")
		}

		group.Go(func() error {
			accountEntries, err := l.rw.GetAccountTransactions(ctx, account, opts)
			if err != nil {
				return err
			}

			mu.Lock()
			entries = append(entries, accountEntries...)
			mu.Unlock()

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return nil, err
	}

	// If we only have one address to look up, then we don't need to re-sort
	// or filter the set for duplicates, as the database layer already does that.
	//
	// Otherwise, since we're merging history, we have to do some extra work.
	if len(addresses) <= 1 {
		return entries, nil
	}

	var sortable sort.Interface = model.SortableEntries(entries)
	if opts.Descending {
		sortable = sort.Reverse(sortable)
	}
	sort.Sort(sortable)

	// Since the entries are sorted, we only need to look at
	// adjacent entries to determine if there's duplicates.
	uniqueEntries := make([]*model.Entry, 0, len(entries))
	for i := 0; i < len(entries); i++ {
		if i > 0 && proto.Equal(entries[i-1], entries[i]) {
			continue
		}

		uniqueEntries = append(uniqueEntries, entries[i])
	}

	limit := len(uniqueEntries)
	if opts.Limit > 0 {
		limit = int(math.Min(float64(len(uniqueEntries)), float64(opts.Limit)))
	}
	return uniqueEntries[:limit], nil
}

func historyItemFromEntry(entry *model.Entry) (*transactionpb.HistoryItem, error) {
	txID, err := entry.GetTxID()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get transaction id")
	}
	orderingKey, err := entry.GetOrderingKey()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get ordering key")
	}

	id := &commonpbv4.TransactionId{
		Value: txID[:],
	}
	cursor := &transactionpb.Cursor{
		Value: orderingKey,
	}

	switch e := entry.Kind.(type) {
	case *model.Entry_Stellar:
		var envelope xdr.TransactionEnvelope
		if err := envelope.UnmarshalBinary(e.Stellar.EnvelopeXdr); err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal envelope")
		}
		payments, err := paymentsFromEnvelope(envelope)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get payments from envelope")
		}
		return &transactionpb.HistoryItem{
			TransactionId: id,
			Cursor:        cursor,
			Payments:      payments,
			RawTransaction: &transactionpb.HistoryItem_StellarTransaction{
				StellarTransaction: &commonpbv4.StellarTransaction{
					EnvelopeXdr: e.Stellar.EnvelopeXdr,
					ResultXdr:   e.Stellar.ResultXdr,
				},
			},
		}, nil
	case *model.Entry_Solana:
		var txn solana.Transaction
		if err := txn.Unmarshal(e.Solana.Transaction); err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal transaction")
		}

		var txErr *commonpbv4.TransactionError
		if len(e.Solana.TransactionError) > 0 {
			var raw interface{}
			d := json.NewDecoder(bytes.NewBufferString(string(e.Solana.TransactionError)))
			if err := d.Decode(&raw); err != nil {
				return nil, errors.Wrap(err, "failed to unmarshal transaction error")
			}
			parsed, err := solana.ParseTransactionError(raw)
			if err != nil {
				return nil, errors.Wrap(err, "failed to parse transaction error")
			}
			txErr, err = solanautil.MapTransactionError(*parsed)
			if err != nil {
				return nil, errors.Wrap(err, "failed to map transaction error")
			}
		}

		return &transactionpb.HistoryItem{
			TransactionId: id,
			Cursor:        cursor,
			Payments:      paymentsFromTransaction(txn),
			RawTransaction: &transactionpb.HistoryItem_SolanaTransaction{
				SolanaTransaction: &commonpbv4.Transaction{
					Value: e.Solana.Transaction,
				},
			},
			TransactionError: txErr,
		}, nil
	default:
		return nil, errors.New("unsupported entry type")
	}
}

func paymentsFromTransaction(txn solana.Transaction) []*transactionpb.HistoryItem_Payment {
	payments := make([]*transactionpb.HistoryItem_Payment, 0, len(txn.Message.Instructions))
	for i := range txn.Message.Instructions {
		transfer, err := token.DecompileTransferAccount(txn.Message, i)
		if err != nil {
			continue
		}

		payments = append(payments, &transactionpb.HistoryItem_Payment{
			Source: &commonpbv4.SolanaAccountId{
				Value: transfer.Source,
			},
			Destination: &commonpbv4.SolanaAccountId{
				Value: transfer.Destination,
			},
			Amount: int64(transfer.Amount),
			Index:  uint32(i),
		})
	}

	return payments
}

func paymentsFromEnvelope(envelope xdr.TransactionEnvelope) ([]*transactionpb.HistoryItem_Payment, error) {
	// todo: can we re-use from client sdk?
	payments := make([]*transactionpb.HistoryItem_Payment, 0, len(envelope.Tx.Operations))
	for i, op := range envelope.Tx.Operations {
		// Currently we only support payment operations in this RPC.
		//
		// We could potentially expand this to CreateAccount functions,
		// as well as merge account. However, GetTransaction() is primarily
		// only used for payments.
		if op.Body.PaymentOp == nil {
			continue
		}

		var source xdr.AccountId
		if op.SourceAccount != nil {
			source = *op.SourceAccount
		} else {
			source = envelope.Tx.SourceAccount
		}

		sender, err := publicKeyFromStellarXDR(source)
		if err != nil {
			return payments, errors.Wrap(err, "invalid sender account")
		}
		dest, err := publicKeyFromStellarXDR(op.Body.PaymentOp.Destination)
		if err != nil {
			return payments, errors.Wrap(err, "invalid destination account")
		}

		payments = append(payments, &transactionpb.HistoryItem_Payment{
			Source: &commonpbv4.SolanaAccountId{
				Value: sender,
			},
			Destination: &commonpbv4.SolanaAccountId{
				Value: dest,
			},
			Amount: int64(op.Body.PaymentOp.Amount),
			Index:  uint32(i),
		})
	}

	return payments, nil
}

func publicKeyFromStellarXDR(id xdr.AccountId) ([]byte, error) {
	v, ok := id.GetEd25519()
	if !ok {
		return nil, errors.New("xdr.AccountId not an ed25519 key")
	}

	pub := make([]byte, 32)
	copy(pub, v[:])
	return pub, nil
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

	orderKey, err := model.OrderingKeyFromCursor(model.KinVersion(c.Value[0]), string(c.Value))
	if err != nil {
		return nil, errors.Wrap(err, "failed to get ordering key from transactionpb.Cursor")
	}
	return orderKey, nil
}
