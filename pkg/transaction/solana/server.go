package solana

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
	"github.com/kinecosystem/go/clients/horizon"
	"github.com/mr-tron/base58"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v4"
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v4"
	transactionpb "github.com/kinecosystem/agora-api/genproto/transaction/v4"

	"github.com/kinecosystem/agora/pkg/account/solana/accountinfo"
	"github.com/kinecosystem/agora/pkg/invoice"
	"github.com/kinecosystem/agora/pkg/migration"
	"github.com/kinecosystem/agora/pkg/solanautil"
	"github.com/kinecosystem/agora/pkg/transaction"
	"github.com/kinecosystem/agora/pkg/transaction/dedupe"
	"github.com/kinecosystem/agora/pkg/transaction/history"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
	"github.com/kinecosystem/agora/pkg/webhook/events"
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
	scSubmit        solana.Client
	tc              *token.Client
	loader          *loader
	invoiceStore    invoice.Store
	history         history.ReaderWriter
	authorizer      transaction.Authorizer
	migrator        migration.Migrator
	infoCache       accountinfo.Cache
	eventsSubmitter events.Submitter
	deduper         dedupe.Deduper

	token      ed25519.PublicKey
	subsidizer ed25519.PrivateKey

	hc horizon.ClientInterface

	// todo: could use sync map, shouldn't be an issue for now
	cacheMu         sync.RWMutex
	rentExemptCache map[uint64]uint64
}

func New(
	sc solana.Client,
	scSubmit solana.Client,
	invoiceStore invoice.Store,
	history history.ReaderWriter,
	committer ingestion.Committer,
	authorizer transaction.Authorizer,
	migrator migration.Migrator,
	infoCache accountinfo.Cache,
	eventsSubmitter events.Submitter,
	deduper dedupe.Deduper,
	tokenAccount ed25519.PublicKey,
	subsidizer ed25519.PrivateKey,
	hc horizon.ClientInterface,
) transactionpb.TransactionServer {
	return &server{
		log:      logrus.StandardLogger().WithField("type", "transaction/solana/server"),
		sc:       sc,
		scSubmit: scSubmit,
		tc:       token.NewClient(sc, tokenAccount),
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
		migrator:        migrator,
		infoCache:       infoCache,
		eventsSubmitter: eventsSubmitter,
		deduper:         deduper,
		token:           tokenAccount,
		subsidizer:      subsidizer,
		hc:              hc,
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
	hash, err := s.scSubmit.GetRecentBlockhash()
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
	var transfers []*token.DecompiledTransferAccount
	var transferAccountPairs [][]ed25519.PublicKey

	transferStates := make(map[string]int64)

	//
	// Parse out Transfer() and Memo() instructions.
	//
	switch len(txn.Message.Instructions) {
	case 0:
		return nil, status.Error(codes.InvalidArgument, "no instructions specified")
	case 1:
		transfers = make([]*token.DecompiledTransferAccount, 1)
		transfers[0], err = token.DecompileTransferAccount(txn.Message, 0)
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

		transfers = make([]*token.DecompiledTransferAccount, len(txn.Message.Instructions)-offset)
		for i := 0; i < len(txn.Message.Instructions)-offset; i++ {
			transfers[i], err = token.DecompileTransferAccount(txn.Message, i+offset)
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
	if err := migration.MigrateTransferAccounts(ctx, s.hc, s.migrator, transferAccountPairs...); err != nil && err != migration.ErrNotFound {
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
		log.WithField("tx", base64.StdEncoding.EncodeToString(tx.ID)).Info("Storing invoice")
		if err := s.invoiceStore.Put(ctx, tx.ID, tx.InvoiceList); err != nil && err != invoice.ErrExists {
			log.WithError(err).Warn("failed to store invoice list")
			return nil, status.Errorf(codes.Internal, "failed to store invoice list")
		}
	}

	// Instead of directly invalidating, we update it to the predicted amount.
	speculativeStates := s.speculativeLoad(ctx, transferStates, solanautil.CommitmentFromProto(req.Commitment))

	select {
	case <-ctx.Done():
		submitTransactionsCancelled.Inc()
		return nil, status.Error(codes.Canceled, "caller cancelled")
	default:
	}

	submitStart := time.Now()
	var submitResult transactionpb.SubmitTransactionResponse_Result
	sig, stat, err := s.scSubmit.SubmitTransaction(txn, solanautil.CommitmentFromProto(req.Commitment))
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

			log.WithError(stat.ErrorResult).Info("failed to submit transaction")

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
		s.speculativeWrite(forkedCtx, speculativeStates)
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

	if err := s.eventsSubmitter.Submit(forkedCtx, entry); err != nil {
		log.WithError(err).Warn("failed to forward webhook")
		eventsWebhookFailures.Inc()
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

	if err := s.migrator.InitiateMigration(ctx, req.AccountId.Value, false, solana.CommitmentSingle); err != nil && err != migration.ErrNotFound {
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

// speculativeLoad loads existing account states, and merges it with the transfer states.
//
// Currently, speculativeLoad uses a chained strategy, whereby we re-use the speculations
// from previous executions if they exist before using on chain information. This approach
// has some drawbacks, but it is sufficient for what we want in the current 'state of kin'.
//
// Benefits: This approach is fairly simple, and solves the problem of doing "rapid" transfers
// in succession. Currently, the time from Submit() to the effect being visible on chain tends
// to be from 5 - 30 seconds. If we were to submit multiple transfers for the same account using
// the block chain information (inside this window), the speculations would all be the same,
// rather than being additive. By using the cached values first, we get the additive property.
//
// Cons: If the transaction is rolled back, or external (to agora) transactions are submitted,
// then our speculations will be wrong. Worse, if we continually speculate within the cache window,
// we will never have a chance to resync with/rebase off of the block chain state. Since everyone
// who uses agora's GetAccountBalance() also uses agoras Submit() (currently), we don't care as
// much about the latter case. In the former case, we rely (maybe incorrectly) on:
//
//  1. The simulation phase of the transaction will yield any early failures, reducing the chance
//     of rollback at a later point in time.
//  2. Regular observers of accounts don't have extended transfer 'sessions'. For example, a user may
//     receive and earn, open the app, and send it back. Or they may just send kin to a few places. This
//     activity is likely to occur within a small time window, with a decent (cache expiry+) gap between
//     the next window, giving us a chance to rsync.
//
// (2)'s caveat is developer wallets (wallet that have a sustained transfers) are at risk of divergence
// from our API. So far they mostly been using explorer, but they also are more tolerant.
//
// Finally, transactions never use our data, so a user still cannot send kin they do not have.
//
// Future work would involve recording speculative predictions as diffs, and periodically re-syncing
// with the underlying blockchain, and discarding all speculations before the sync (based on slot.)
func (s *server) speculativeLoad(ctx context.Context, transferState map[string]int64, commitment solana.Commitment) (newState map[string]int64) {
	log := s.log.WithField("method", "speculativeLoad")

	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(transferState))

	newState = make(map[string]int64)

	for accountKey, diff := range transferState {
		go func(a string, b int64) {
			defer wg.Done()

			info, err := s.infoCache.Get(ctx, ed25519.PublicKey(a))
			if err != nil && err != accountinfo.ErrAccountInfoNotFound {
				speculativeUpdateFailures.WithLabelValues("info_cache_load").Inc()
				infoCacheFailures.WithLabelValues("speculative_load").Inc()
				log.WithError(err).Warn("failed to load cache info for speculative execution")
			}

			if info != nil {
				mu.Lock()
				newState[a] += info.Balance + b
				mu.Unlock()
				return
			}

			account, err := s.tc.GetAccount(ed25519.PublicKey(a), commitment)
			if err == token.ErrAccountNotFound || err == token.ErrInvalidTokenAccount {
				return
			} else if err != nil {
				speculativeUpdateFailures.WithLabelValues("solana_load").Inc()
				log.WithError(err).Warn("failed to load token info for speculative execution")
				return
			} else if account == nil {
				speculativeUpdateFailures.WithLabelValues("solana_load_empty").Inc()
				return
			}

			mu.Lock()
			newState[a] += int64(account.Amount) + b
			mu.Unlock()
		}(accountKey, diff)
	}

	wg.Wait()
	return newState
}

func (s *server) speculativeWrite(ctx context.Context, speculativeStates map[string]int64) {
	log := s.log.WithField("method", "speculativeWrite")

	var wg sync.WaitGroup
	wg.Add(len(speculativeStates))

	for accountKey, balance := range speculativeStates {
		go func(a string, b int64) {
			defer wg.Done()

			err := s.infoCache.Put(ctx, &accountpb.AccountInfo{
				AccountId: &commonpb.SolanaAccountId{
					Value: []byte(a),
				},
				Balance: b,
			})
			if err != nil {
				speculativeUpdateFailures.WithLabelValues("speculative_write").Inc()
				log.WithError(err).Warn("failed to update info cache")
			} else {
				speculativeUpdates.Inc()
			}
		}(accountKey, balance)
	}

	wg.Wait()
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
	speculativeUpdates = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "speculative_updates",
	})
	speculativeUpdateFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "speculative_update_failures",
	}, []string{"type"})
	infoCacheFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "info_cache_failures",
	}, []string{"type"})
	eventsWebhookFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "agora",
		Name:      "submit_transaction_webhook_failures",
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
	submitTxResultCounter = metrics.Register(submitTxCounter).(*prometheus.CounterVec)
	submitTimingsByCode = metrics.Register(submitTimingsByCode).(*prometheus.HistogramVec)
	speculativeUpdates = metrics.Register(speculativeUpdates).(prometheus.Counter)
	speculativeUpdateFailures = metrics.Register(speculativeUpdateFailures).(*prometheus.CounterVec)
	infoCacheFailures = metrics.Register(infoCacheFailures).(*prometheus.CounterVec)
	eventsWebhookFailures = metrics.Register(eventsWebhookFailures).(prometheus.Counter)
	submitTransactionsCancelled = metrics.Register(submitTransactionsCancelled).(prometheus.Counter)
	transferByDest = metrics.Register(transferByDest).(*prometheus.CounterVec)
	dedupesByType = metrics.Register(dedupesByType).(*prometheus.CounterVec)
	dedupeTransitionFailures = metrics.Register(dedupeTransitionFailures).(*prometheus.CounterVec)
}
