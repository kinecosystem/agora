package stellar

import (
	"bytes"
	"context"
	"encoding/base64"
	"strconv"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/kinecosystem/agora-common/retry"
	"github.com/kinecosystem/agora-common/retry/backoff"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go/clients/horizonclient"
	hProtocol "github.com/stellar/go/protocols/horizon"

	"github.com/kinecosystem/agora/pkg/transaction/history"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

type ingestor struct {
	log               *logrus.Entry
	name              string
	version           model.KinVersion
	client            horizonclient.ClientInterface
	networkPassphrase string
}

func New(name string, version model.KinVersion, client horizonclient.ClientInterface, networkPassphrase string) ingestion.Ingestor {
	return &ingestor{
		log: logrus.StandardLogger().WithFields(logrus.Fields{
			"type":    "transaction/history/ingestion/stellar",
			"version": version,
		}),
		name:              name,
		version:           version,
		client:            client,
		networkPassphrase: networkPassphrase,
	}
}

// Name implements ingestion.Ingestor.Name.
func (i *ingestor) Name() string {
	return i.name
}

// Ingest implements ingestion.Ingestor.Ingest.
func (i *ingestor) Ingest(ctx context.Context, w history.Writer, parent ingestion.Pointer) (ingestion.ResultQueue, error) {
	_, err := cursorFromPointer(parent)
	if err != nil {
		return nil, err
	}

	// todo(config): allow for a customizable buffer?
	queue := make(chan (<-chan ingestion.Result), 8)

	// StreamLedgers synchronously calls ledgerHandler in ascending order for each ledger,
	// such that the execution flow is as follows:
	//
	//     ledgerHandler(P+1),
	//     ledgerHandler(P+2),
	//      ...
	//     ledgerHandler(P+n),
	//
	// where P is the parent pointer.
	//
	// Each ingestion.Result produced by a ledgerHandler needs to have both the parent
	// and the processed ledger pointer. Unfortunately, the parent field in hProtocol.Ledger
	// is a hash, _not_ a sequence number, which cannot be translated to a pointer without
	// a lookup.
	//
	// However, since ledgerHandler is executed synchronously and in order, we can
	// abuse the 'global' parent pointer, by simply advancing it in local context each time
	// ledgerHandler is called. We simply capture the (parent, block) state before starting
	// the async processing of the ledger, and use those values when the processing is complete.
	ledgerHandler := func(l hProtocol.Ledger) {
		ledgerPtr := pointerFromSequence(i.version, uint32(l.Sequence))

		// if the ledgerPtr is the same as our parent, then it's already
		// been processed, so we can save some work. this generally occurs
		// on the very first ledger in the stream.
		if bytes.Equal(ledgerPtr, parent) {
			return
		}

		resultCh := make(chan ingestion.Result, 1)
		queue <- resultCh

		result := ingestion.Result{
			Parent: parent,
			Block:  ledgerPtr,
		}
		parent = ledgerPtr

		go func() {
			if err := i.processLedger(l, w); err != nil {
				result.Err = err
			}

			resultCh <- result
			close(resultCh)
		}()
	}

	go func() {
		defer close(queue)

		// todo: should we just validate this on initial ingestion
		//       and then panic on subsequent calls, as we've been
		//       managing the end-to-end lifetime of the parent pointer.
		errInvalidPointer := errors.New("invalid pointer")
		_, err := retry.Retry(
			func() error {
				cursor, err := cursorFromPointer(parent)
				if err != nil {
					return errInvalidPointer
				}

				req := horizonclient.LedgerRequest{
					Cursor: cursor,
					Order:  horizonclient.OrderAsc,
				}

				// StreamLedgers _should_ only return without an error if the context was cancelled.
				//
				// Otherwise, our retry will resume where we left off as it uses parent, which is being
				// updated by ledgerHandler. If some gap was detected as a result of this, the caller will
				// cancel the context, and re-construct the ingestion process (if still relevant).
				if err := i.client.StreamLedgers(ctx, req, ledgerHandler); err != nil {
					return err
				}

				return nil
			},
			retry.NonRetriableErrors(errInvalidPointer, context.Canceled),
			retry.BackoffWithJitter(backoff.BinaryExponential(time.Second), 30*time.Second, 0.1),
		)
		i.log.WithError(err).Info("ingestion stream closed")
	}()

	return queue, nil
}

func (i *ingestor) processLedger(ledger hProtocol.Ledger, w history.Writer) error {
	page, err := i.client.Transactions(horizonclient.TransactionRequest{
		ForLedger: uint(ledger.Sequence),
	})
	if err != nil {
		return errors.Wrap(err, "failed to get ledger transactions")
	}

	closeTime, err := ptypes.TimestampProto(ledger.ClosedAt)
	if err != nil {
		return errors.Wrap(err, "invalid close time")
	}

	for _, txn := range page.Embedded.Records {
		envelopeBytes, err := base64.StdEncoding.DecodeString(txn.EnvelopeXdr)
		if err != nil {
			return errors.Wrap(err, "failed to parse envelope xdr")
		}
		resultBytes, err := base64.StdEncoding.DecodeString(txn.ResultXdr)
		if err != nil {
			return errors.Wrap(err, "failed to parse result xdr")
		}
		pagingToken, err := strconv.ParseUint(txn.PagingToken(), 10, 64)
		if err != nil {
			return errors.Wrap(err, "failed to parse paging token")
		}

		entry := &model.Entry{
			Version: i.version,
			Kind: &model.Entry_Stellar{
				Stellar: &model.StellarEntry{
					Ledger:            uint64(ledger.Sequence),
					PagingToken:       pagingToken,
					LedgerCloseTime:   closeTime,
					NetworkPassphrase: i.networkPassphrase,
					EnvelopeXdr:       envelopeBytes,
					ResultXdr:         resultBytes,
				},
			},
		}

		if err := w.Write(context.Background(), entry); err != nil {
			return errors.Wrap(err, "failed to write txn")
		}
	}

	return nil
}
