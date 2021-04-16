package server

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"strings"
	"sync"
	"time"

	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/agora-common/kin/version"
	"github.com/kinecosystem/agora-common/metrics"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/memo"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/mr-tron/base58"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/kinecosystem/agora-api/genproto/common/v4"
	transactionpb "github.com/kinecosystem/agora-api/genproto/transaction/v4"

	"github.com/kinecosystem/agora/pkg/account/specstate"
	"github.com/kinecosystem/agora/pkg/events"
	"github.com/kinecosystem/agora/pkg/events/eventspb"
	"github.com/kinecosystem/agora/pkg/invoice"
	"github.com/kinecosystem/agora/pkg/migration"
	"github.com/kinecosystem/agora/pkg/solanautil"
	"github.com/kinecosystem/agora/pkg/transaction"
	"github.com/kinecosystem/agora/pkg/transaction/dedupe"
	"github.com/kinecosystem/agora/pkg/transaction/history"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
	webevents "github.com/kinecosystem/agora/pkg/webhook/events"
	"github.com/kinecosystem/agora/pkg/webhook/signtransaction"
)

var destWhitelist = map[string]struct{}{
	"Fapzahf3E91zAvr1yvNw3mYLv7t2DR6EnT8xjY74fT7C": {}, // Rave
	"4bRrapNAChmZMLugZacanKfqW5KyHygcWEReBVNiMTUU": {}, // Rave (owner)
	"CncYnFygz323VNY6okoiv6ycByLumgHzXSBFzXDDFNEZ": {}, // Peerbet
	"BUS5SyrVLhgakivdRcmZE5F69HdR8xJBjHTiF2mqdKpt": {}, // Peerbet (owner)
	"2K8XpTqVAheX9cF2niwkTQQBajEwr84TeP34wiYUCoLy": {}, // PauseFor
	"3rad7aFPdJS3CkYPSphtDAWCNB8BYpV2yc7o5ZjFQbDb": {}, // PauseFor (owner)
	"7cqCpmzfZphbhzctXLJVabxQecFtYp6Bg4vQXo11SiNM": {}, // Poppin
	"ejsuFLdZo3YBu4qeuSw9ozbFPwPaUd3Xc2PPuDRpPdS":  {}, // Poppin (owner)
}

type server struct {
	log             *logrus.Entry
	sc              solana.Client
	tc              *token.Client
	loader          *loader
	invoiceStore    invoice.Store
	history         history.ReaderWriter
	authorizer      transaction.Authorizer
	migrationLoader migration.Loader
	migrator        migration.Migrator
	webEvents       webevents.Submitter
	streamEvents    events.Submitter
	deduper         dedupe.Deduper
	specStateLoader *specstate.Loader

	token      ed25519.PublicKey
	subsidizer ed25519.PrivateKey

	// todo: could use sync map, shouldn't be an issue for now
	cacheMu         sync.RWMutex
	rentExemptCache map[uint64]uint64
}

func New(
	sc solana.Client,
	invoiceStore invoice.Store,
	history history.ReaderWriter,
	committer ingestion.Committer,
	authorizer transaction.Authorizer,
	migrationLoader migration.Loader,
	migrator migration.Migrator,
	webEvents webevents.Submitter,
	streamEvents events.Submitter,
	deduper dedupe.Deduper,
	specStateLoader *specstate.Loader,
	tokenAccount ed25519.PublicKey,
	subsidizer ed25519.PrivateKey,
) transactionpb.TransactionServer {
	return &server{
		log: logrus.StandardLogger().WithField("type", "transaction/solana/server"),
		sc:  sc,
		tc:  token.NewClient(sc, tokenAccount),
		loader: newLoader(
			sc,
			history,
			committer,
			invoiceStore,
			tokenAccount,
		),
		history:         history,
		invoiceStore:    invoiceStore,
		authorizer:      authorizer,
		migrationLoader: migrationLoader,
		migrator:        migrator,
		webEvents:       webEvents,
		streamEvents:    streamEvents,
		deduper:         deduper,
		specStateLoader: specStateLoader,
		token:           tokenAccount,
		subsidizer:      subsidizer,
		rentExemptCache: make(map[uint64]uint64),
	}
}

// GetServiceConfig returns the service and token parameters for the token.
func (s *server) GetServiceConfig(_ context.Context, _ *transactionpb.GetServiceConfigRequest) (*transactionpb.GetServiceConfigResponse, error) {
	return &transactionpb.GetServiceConfigResponse{
		Token: &commonpb.SolanaAccountId{
			Value: s.token,
		},
		TokenProgram: &commonpb.SolanaAccountId{
			Value: token.ProgramKey,
		},
		SubsidizerAccount: &commonpb.SolanaAccountId{
			Value: s.subsidizer.Public().(ed25519.PublicKey),
		},
	}, nil
}

func (s *server) GetMinimumKinVersion(ctx context.Context, _ *transactionpb.GetMinimumKinVersionRequest) (*transactionpb.GetMinimumKinVersionResponse, error) {
	desired, err := version.GetCtxDesiredVersion(ctx)
	if err == nil {
		return &transactionpb.GetMinimumKinVersionResponse{
			Version: uint32(desired),
		}, nil
	}

	v, err := version.GetCtxKinVersion(ctx)
	if err != nil {
		v = version.KinVersion3
	}

	if v == version.KinVersion2 {
		return &transactionpb.GetMinimumKinVersionResponse{Version: uint32(2)}, nil
	} else {
		return &transactionpb.GetMinimumKinVersionResponse{Version: uint32(4)}, nil
	}
}

// GetRecentBlockhash returns a recent block hash from the underlying network,
// which should be used when crafting transactions. If a transaction fails, it
// is recommended that a new block hash is retrieved.
func (s *server) GetRecentBlockhash(_ context.Context, _ *transactionpb.GetRecentBlockhashRequest) (*transactionpb.GetRecentBlockhashResponse, error) {
	hash, err := s.sc.GetRecentBlockhash()
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to get recent block hash")
	}

	return &transactionpb.GetRecentBlockhashResponse{
		Blockhash: &commonpb.Blockhash{
			Value: hash[:],
		},
	}, nil
}

// GetMinimumBalanceForRentExemption returns the minimum amount of lamports that
// must be in an account for it not to be garbage collected.
func (s *server) GetMinimumBalanceForRentExemption(_ context.Context, req *transactionpb.GetMinimumBalanceForRentExemptionRequest) (*transactionpb.GetMinimumBalanceForRentExemptionResponse, error) {
	accountSize := req.Size

	// todo: remove temporary patch for account size
	// Use token.AccountSize as patch for account creation bug in the Go client
	if accountSize == 0 {
		accountSize = token.AccountSize
	}

	s.cacheMu.RLock()
	// todo: we may want a ttl, but this in theory only ever goes down.
	cached, ok := s.rentExemptCache[accountSize]
	s.cacheMu.RUnlock()

	if ok {
		return &transactionpb.GetMinimumBalanceForRentExemptionResponse{
			Lamports: cached,
		}, nil
	}

	// todo(perf): could aggressively cache this.
	lamports, err := s.sc.GetMinimumBalanceForRentExemption(accountSize)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to get minimum balance for rent exemption")
	}

	s.cacheMu.Lock()
	s.rentExemptCache[accountSize] = lamports
	s.cacheMu.Unlock()

	return &transactionpb.GetMinimumBalanceForRentExemptionResponse{
		Lamports: lamports,
	}, nil
}

// SubmitTransaction submits a transaction.
//
// See: https://github.com/kinecosystem/agora-api/blob/master/spec/memo.md
func (s *server) SubmitTransaction(ctx context.Context, req *transactionpb.SubmitTransactionRequest) (*transactionpb.SubmitTransactionResponse, error) {
	log := s.log.WithField("method", "SubmitTransaction")

	submitTxCounter.Inc()

	var txn solana.Transaction
	if err := txn.Unmarshal(req.Transaction.Value); err != nil {
		log.WithError(err).Debug("bad transaction encoding")
		return nil, status.Error(codes.InvalidArgument, "bad transaction encoding")
	}

	var err error
	var txMemo *memo.DecompiledMemo
	var transfers []*token.DecompiledTransfer
	var transferAccountPairs [][]ed25519.PublicKey

	transferStates := make(map[string]int64)

	//
	// Parse out Transfer() and Memo() instructions.
	//
	switch len(txn.Message.Instructions) {
	case 0:
		return nil, status.Error(codes.InvalidArgument, "no instructions specified")
	case 1:
		transfers = make([]*token.DecompiledTransfer, 1)
		transfers[0], err = token.DecompileTransfer(txn.Message, 0)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid transfer instruction")
		}
		transferAccountPairs = append(transferAccountPairs, []ed25519.PublicKey{transfers[0].Source, transfers[0].Destination})

		transferStates[string(transfers[0].Source)] -= int64(transfers[0].Amount)
		transferStates[string(transfers[0].Destination)] += int64(transfers[0].Amount)

		// note: this really should be 'unique', but let's go by transfer for now.
		destKey := base58.Encode(transfers[0].Destination)
		if _, ok := destWhitelist[destKey]; ok {
			transferByDest.WithLabelValues(destKey).Inc()
		}
	default:
		var offset int
		if m, err := memo.DecompileMemo(txn.Message, 0); err == nil {
			txMemo = m
			offset = 1
		}

		transfers = make([]*token.DecompiledTransfer, len(txn.Message.Instructions)-offset)
		for i := 0; i < len(txn.Message.Instructions)-offset; i++ {
			transfers[i], err = token.DecompileTransfer(txn.Message, i+offset)
			if err != nil {
				return nil, status.Error(codes.InvalidArgument, "invalid transfer instruction")
			}
			transferAccountPairs = append(transferAccountPairs, []ed25519.PublicKey{transfers[i].Source, transfers[i].Destination})

			transferStates[string(transfers[i].Source)] -= int64(transfers[i].Amount)
			transferStates[string(transfers[i].Destination)] += int64(transfers[i].Amount)

			// note: this really should be 'unique', but let's go by transfer for now.
			destKey := base58.Encode(transfers[i].Destination)
			if _, ok := destWhitelist[destKey]; ok {
				transferByDest.WithLabelValues(destKey).Inc()
			}
		}

		if req.InvoiceList != nil && len(req.InvoiceList.Invoices) != len(transfers) {
			return nil, status.Error(codes.InvalidArgument, "invoice count does not match transfer count")
		}
	}

	log.Debug("Triggering migration batch")

	// todo: migration timings, maybe could be global by scope
	if err := migration.MigrateTransferAccounts(ctx, s.migrationLoader, s.migrator, transferAccountPairs...); err != nil && err != migration.ErrNotFound {
		return nil, status.Errorf(codes.Internal, "failed to migrate transfer accounts: %v", err)
	}

	//
	// Subsidize transaction, if applicable
	//
	if len(s.subsidizer) > 0 {
		if bytes.Equal(txn.Message.Accounts[0], s.subsidizer.Public().(ed25519.PublicKey)) {
			if err := txn.Sign(s.subsidizer); err != nil {
				return nil, status.Error(codes.Internal, "failed to co-sign txn")
			}
		}

		for i := range transfers {
			if bytes.Equal(transfers[i].Source, s.subsidizer) {
				return nil, status.Errorf(codes.InvalidArgument, "sender at transaction %d was service subsidizer", i)
			}
		}
	} else if bytes.Equal(txn.Signatures[0][:], make([]byte, ed25519.SignatureSize)) {
		submitTxResultCounter.WithLabelValues(strings.ToLower(transactionpb.SubmitTransactionResponse_PAYER_REQUIRED.String())).Inc()
		return &transactionpb.SubmitTransactionResponse{
			Result: transactionpb.SubmitTransactionResponse_PAYER_REQUIRED,
		}, nil
	}

	log = log.WithField("sig", base64.StdEncoding.EncodeToString(txn.Signature()))

	//
	// Assemble transaction.Transaction
	//
	tx := transaction.Transaction{
		Version:     4,
		ID:          txn.Signature(),
		InvoiceList: req.InvoiceList,
		OpCount:     len(transfers),
		SignRequest: nil,
	}
	tx.SignRequest, err = signtransaction.CreateSolanaRequest(txn, req.InvoiceList)
	if err != nil {
		log.WithError(err).Warn("failed to convert request for signing")
		return nil, status.Error(codes.Internal, "failed to submit transaction")
	}
	if txMemo != nil {
		if raw, err := base64.StdEncoding.DecodeString(string(txMemo.Data)); err == nil {
			var m kin.Memo
			copy(m[:], raw)

			if kin.IsValidMemoStrict(m) {
				tx.Memo.Memo = &m
			} else {
				str := string(txMemo.Data)
				tx.Memo.Text = &str
			}
		} else {
			str := string(txMemo.Data)
			tx.Memo.Text = &str
		}
	}

	//
	// Check our duplicate stores to see if a transaction has already been submitted
	// with this id.
	//
	// Note: empty dedupe id is a noop to dedupers.
	//
	dedupeInfo := &dedupe.Info{
		Signature:      txn.Signature(),
		SubmissionTime: time.Now(),
	}
	prev, err := s.deduper.Dedupe(ctx, req.DedupeId, dedupeInfo)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to check deduper")
	}

	// If there is a previous 'claim' to the dedupe 'session', then we should
	// not proceed with processing here.
	if prev != nil {
		var resp *transactionpb.SubmitTransactionResponse

		// If we have a previous response, then we're terminal and can just
		// return that.
		//
		// Otherwise, we only continue if the transaction was
		// the same as before, allowing retries
		if prev.Response != nil {
			dedupesByType.WithLabelValues("final").Inc()
			resp = prev.Response
		} else {
			dedupesByType.WithLabelValues("concurrent").Inc()
			resp = &transactionpb.SubmitTransactionResponse{
				Result: transactionpb.SubmitTransactionResponse_ALREADY_SUBMITTED,
				Signature: &commonpb.TransactionSignature{
					Value: prev.Signature,
				},
			}
		}

		return resp, nil
	}

	var noClearDedupe bool
	defer func() {
		if noClearDedupe {
			return
		}

		if err := s.deduper.Delete(context.Background(), req.DedupeId); err != nil {
			dedupeTransitionFailures.WithLabelValues("delete").Inc()
			log.WithError(err).
				WithField("id", base64.StdEncoding.EncodeToString(req.DedupeId)).
				Warn("failed to delete dedupe")
		}
	}()

	//
	// Authorization
	//
	result, err := s.authorizer.Authorize(ctx, tx)
	if err != nil {
		return nil, err
	}

	switch result.Result {
	case transaction.AuthorizationResultOK:
	case transaction.AuthorizationResultInvoiceError:
		submitTxResultCounter.WithLabelValues(strings.ToLower(transactionpb.SubmitTransactionResponse_INVOICE_ERROR.String())).Inc()
		return &transactionpb.SubmitTransactionResponse{
			Result: transactionpb.SubmitTransactionResponse_INVOICE_ERROR,
			Signature: &commonpb.TransactionSignature{
				Value: tx.ID,
			},
			InvoiceErrors: result.InvoiceErrors,
		}, nil
	case transaction.AuthorizationResultRejected:
		submitTxResultCounter.WithLabelValues(strings.ToLower(transactionpb.SubmitTransactionResponse_REJECTED.String())).Inc()
		return &transactionpb.SubmitTransactionResponse{
			Result: transactionpb.SubmitTransactionResponse_REJECTED,
			Signature: &commonpb.TransactionSignature{
				Value: tx.ID,
			},
		}, nil
	default:
		log.WithField("result", result.Result).Warn("unexpected authorization result")
		return nil, status.Error(codes.Internal, "unhandled authorization error")
	}

	//
	// Submit and record.
	//
	if tx.InvoiceList != nil {
		// todo: do we want to perform garbage collection for failed transactions that are not in the record?
		log.WithField("tx", base64.StdEncoding.EncodeToString(tx.ID)).Debug("Storing invoice")
		if err := s.invoiceStore.Put(ctx, tx.ID, tx.InvoiceList); err != nil && err != invoice.ErrExists {
			log.WithError(err).Warn("failed to store invoice list")
			return nil, status.Errorf(codes.Internal, "failed to store invoice list")
		}
	}

	// Instead of directly invalidating, we update it to the predicted amount.
	speculativeStates := s.specStateLoader.Load(ctx, transferStates, solanautil.CommitmentFromProto(req.Commitment))

	select {
	case <-ctx.Done():
		submitTransactionsCancelled.Inc()
		return nil, status.Error(codes.Canceled, "caller cancelled")
	default:
	}

	submitStart := time.Now()
	var submitResult transactionpb.SubmitTransactionResponse_Result
	sig, stat, err := s.sc.SubmitTransaction(txn, solanautil.CommitmentFromProto(req.Commitment))
	submitTime := time.Since(submitStart)
	if err != nil {
		log.WithError(err).Warn("unhandled SubmitTransaction")
		submitTimingsByCode.WithLabelValues("unhandled").Observe(float64(submitTime.Seconds()))
		return nil, status.Errorf(codes.Internal, "unhandled error from SubmitTransaction: %v", err)
	}
	if stat.ErrorResult != nil {
		// If it's a duplicate signature, we still want to process the Write()
		// in case that's what failed on an earlier call.
		if solanautil.IsDuplicateSignature(stat.ErrorResult) {
			submitResult = transactionpb.SubmitTransactionResponse_ALREADY_SUBMITTED
		} else {
			// todo: do we want to persist failed transactions at this stage?
			//       if it's not in the simulation stage (which maybe we can disable),
			//       then it will show up in history anyway.
			resp := &transactionpb.SubmitTransactionResponse{
				Result: transactionpb.SubmitTransactionResponse_FAILED,
				Signature: &commonpb.TransactionSignature{
					Value: sig[:],
				},
			}

			log.WithError(stat.ErrorResult).Debug("failed to submit transaction")

			submitTxResultCounter.WithLabelValues(strings.ToLower(submitResult.String())).Inc()

			txError, err := solanautil.MapTransactionError(*stat.ErrorResult)
			if err != nil {
				log.WithError(err).Warn("failed to map transaction error")
				resp.TransactionError = &commonpb.TransactionError{
					Reason: commonpb.TransactionError_UNKNOWN,
				}
			} else {
				resp.TransactionError = txError
			}

			dedupeInfo.Response = resp
			if err := s.deduper.Update(ctx, req.DedupeId, dedupeInfo); err != nil {
				log.WithError(err).Warn("failed to update dedupe info")
			}
			submitTimingsByCode.WithLabelValues(resp.Result.String()).Observe(float64(submitTime.Seconds()))
			return resp, nil
		}
	}

	submitTimingsByCode.WithLabelValues(submitResult.String()).Observe(float64(submitTime.Seconds()))

	// We fork the context here, because we want to take action based on the result
	// of the transaction regardless if the client has cancelled the request.
	forkedCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	entry := &model.Entry{
		Version: model.KinVersion_KIN4,
		Kind: &model.Entry_Solana{
			Solana: &model.SolanaEntry{
				Slot:        stat.Slot,
				Transaction: txn.Marshal(),
			},
		},
	}

	if submitResult == transactionpb.SubmitTransactionResponse_OK {
		s.specStateLoader.Write(forkedCtx, speculativeStates)
	}

	if err := s.history.Write(forkedCtx, entry); err != nil {
		// If we're processing an ALREADY_SUBMITTED, then it's possible we've
		// also already written the entry to history. In this case, we may
		// receive a history.ErrInvalidUpdate, since we don't have any slot
		// information yet.
		//
		// This is ok to ignore, since the stored entry is already up to date.
		if submitResult != transactionpb.SubmitTransactionResponse_ALREADY_SUBMITTED || !errors.Is(err, history.ErrInvalidUpdate) {
			log.WithError(err).Warn("failed to history persist entry")
			return nil, status.Error(codes.Internal, "failed to persist transaction data")
		}
	}

	if err := s.webEvents.Submit(forkedCtx, entry); err != nil {
		log.WithError(err).Warn("failed to forward webhook")
		eventsWebhookFailures.Inc()
	}
	event := &eventspb.Event{
		Kind: &eventspb.Event_TransactionEvent{
			TransactionEvent: &eventspb.TransactionEvent{
				Transaction: txn.Marshal(),
			},
		},
	}
	if err := s.streamEvents.Submit(forkedCtx, event); err != nil {
		log.WithError(err).Warn("failed to forward event stream")
		eventsStreamFailures.Inc()
	}

	submitTxResultCounter.WithLabelValues(strings.ToLower(submitResult.String())).Inc()

	resp := &transactionpb.SubmitTransactionResponse{
		Result: submitResult,
		Signature: &commonpb.TransactionSignature{
			Value: sig[:],
		},
	}

	// Since we have a 'success' response, we do not want to clear the dedupe info.
	noClearDedupe = true
	dedupeInfo.Response = resp
	if err := s.deduper.Update(forkedCtx, req.DedupeId, dedupeInfo); err != nil {
		dedupeTransitionFailures.WithLabelValues("update").Inc()
		log.WithError(err).Warn("failed to update dedupe info")
	}

	return resp, nil
}

// GetTransaction returns a transaction and additional off-chain
// invoice data, if available.
func (s *server) GetTransaction(ctx context.Context, req *transactionpb.GetTransactionRequest) (*transactionpb.GetTransactionResponse, error) {
	log := logrus.WithFields(logrus.Fields{
		"method": "GetTransaction",
		"id":     base64.StdEncoding.EncodeToString(req.TransactionId.Value),
	})

	if len(req.TransactionId.Value) != 32 && len(req.TransactionId.Value) != 64 {
		return nil, status.Error(codes.Internal, "invalid transaction signature")
	}

	resp, err := s.loader.loadTransaction(ctx, req.TransactionId.Value)
	if err != nil {
		log.WithError(err).Warn("failed to load transaction")
		return nil, status.Error(codes.Internal, "failed to load transaction")
	}
	return resp, nil
}

// GetHistory returns the transaction history for an account,
// with additional off-chain invoice data, if available.
func (s *server) GetHistory(ctx context.Context, req *transactionpb.GetHistoryRequest) (*transactionpb.GetHistoryResponse, error) {
	log := logrus.WithFields(logrus.Fields{
		"method":  "GetHistory",
		"account": base64.StdEncoding.EncodeToString(req.AccountId.Value),
	})

	err := s.migrator.InitiateMigration(ctx, req.AccountId.Value, false, solana.CommitmentSingle)
	switch err {
	case nil, migration.ErrNotFound, migration.ErrBurned:
	default:
		return nil, status.Errorf(codes.Internal, "failed to migrate account: %v", err)
	}

	items, err := s.loader.getItems(ctx, req.AccountId.Value, req.Cursor, req.Direction)
	if err != nil {
		log.WithError(err).Warn("failed to get history transactions")
		return nil, status.Error(codes.Internal, "failed to get transactions")
	}

	return &transactionpb.GetHistoryResponse{
		Result: transactionpb.GetHistoryResponse_OK,
		Items:  items,
	}, nil
}

var (
	submitTxCounter     = transaction.SubmitTransactionCounter.WithLabelValues("4")
	submitTimingsByCode = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "agora",
		Name:      "submit_transaction_submit_seconds",
		Help:      "Histogram of submit latency from SubmitTransaction",
		Buckets:   metrics.MinuteDistributionBuckets,
	}, []string{"result"})

	submitTxResultCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "submit_transaction_result",
		Help:      "Number of submit transaction results for OK",
	}, []string{"result"})
	eventsWebhookFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "submit_transaction_webhook_failures",
	})
	eventsStreamFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "submit_transaction_stream_failures",
	})
	submitTransactionsCancelled = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "submit_transactions_cancelled",
	})
	transferByDest = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "transfer_by_dest",
		Help:      "Number of transfers by destination (whitelisted)",
	}, []string{"dest"})
	dedupesByType = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "transfer_dedupes",
		Help:      "Number of deuplications by type",
	}, []string{"type"})
	dedupeTransitionFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "dedupe_transition_failures",
		Help:      "Number of failures to update dedupe info",
	}, []string{"op"})
)

func init() {
	submitTxResultCounter = metrics.Register(submitTxResultCounter).(*prometheus.CounterVec)
	submitTimingsByCode = metrics.Register(submitTimingsByCode).(*prometheus.HistogramVec)
	eventsWebhookFailures = metrics.Register(eventsWebhookFailures).(prometheus.Counter)
	eventsStreamFailures = metrics.Register(eventsStreamFailures).(prometheus.Counter)
	submitTransactionsCancelled = metrics.Register(submitTransactionsCancelled).(prometheus.Counter)
	transferByDest = metrics.Register(transferByDest).(*prometheus.CounterVec)
	dedupesByType = metrics.Register(dedupesByType).(*prometheus.CounterVec)
	dedupeTransitionFailures = metrics.Register(dedupeTransitionFailures).(*prometheus.CounterVec)
}
