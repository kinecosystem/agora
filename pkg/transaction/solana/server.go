package solana

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"

	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/memo"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/kinecosystem/agora-api/genproto/common/v4"
	transactionpb "github.com/kinecosystem/agora-api/genproto/transaction/v4"

	"github.com/kinecosystem/agora/pkg/invoice"
	"github.com/kinecosystem/agora/pkg/solanautil"
	"github.com/kinecosystem/agora/pkg/transaction"
	"github.com/kinecosystem/agora/pkg/transaction/history"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
	"github.com/kinecosystem/agora/pkg/version"
	"github.com/kinecosystem/agora/pkg/webhook/signtransaction"
)

type server struct {
	log          *logrus.Entry
	sc           solana.Client
	tc           *token.Client
	loader       *loader
	invoiceStore invoice.Store
	history      history.ReaderWriter
	authorizer   transaction.Authorizer

	token      ed25519.PublicKey
	subsidizer ed25519.PrivateKey
}

func New(
	sc solana.Client,
	invoiceStore invoice.Store,
	history history.ReaderWriter,
	committer ingestion.Committer,
	authorizer transaction.Authorizer,
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
		history:      history,
		invoiceStore: invoiceStore,
		authorizer:   authorizer,
		token:        tokenAccount,
		subsidizer:   subsidizer,
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

	return &transactionpb.GetMinimumKinVersionResponse{
		Version: 2,
	}, nil
}

// GetRecentBlockhash returns a recent block hash from the underlying network,
// which should be used when crafting transactions. If a transaction fails, it
// is recommended that a new block hash is retrieved.
func (s *server) GetRecentBlockhash(_ context.Context, _ *transactionpb.GetRecentBlockhashRequest) (*transactionpb.GetRecentBlockhashResponse, error) {
	// todo(perf): could cache this with some small window.
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
	// todo(perf): could aggressively cache this.
	lamports, err := s.sc.GetMinimumBalanceForRentExemption(req.Size)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to get minimum balance for rent exemption")
	}

	return &transactionpb.GetMinimumBalanceForRentExemptionResponse{
		Lamports: lamports,
	}, nil
}

// SubmitTransaction submits a transaction.
//
// See: https://github.com/kinecosystem/agora-api/blob/master/spec/memo.md
func (s *server) SubmitTransaction(ctx context.Context, req *transactionpb.SubmitTransactionRequest) (*transactionpb.SubmitTransactionResponse, error) {
	log := s.log.WithField("method", "SubmitTransaction")

	var txn solana.Transaction
	if err := txn.Unmarshal(req.Transaction.Value); err != nil {
		return nil, status.Error(codes.InvalidArgument, "bad transaction encoding")
	}

	var err error
	var txMemo *memo.DecompiledMemo
	var transfers []*token.DecompiledTransferAccount

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
		}

		if req.InvoiceList != nil && len(req.InvoiceList.Invoices) != len(transfers) {
			return nil, status.Error(codes.InvalidArgument, "invoice count does not match transfer count")
		}
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
	tx.SignRequest, err = signtransaction.CreateSolanaRequest(req)
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
	// Authorization
	//
	result, err := s.authorizer.Authorize(ctx, tx)
	if err != nil {
		return nil, err
	}

	switch result.Result {
	case transaction.AuthorizationResultOK:
	case transaction.AuthorizationResultInvoiceError:
		return &transactionpb.SubmitTransactionResponse{
			Result: transactionpb.SubmitTransactionResponse_INVOICE_ERROR,
			Signature: &commonpb.TransactionSignature{
				Value: tx.ID,
			},
			InvoiceErrors: result.InvoiceErrors,
		}, nil
	case transaction.AuthorizationResultRejected:
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

	var submitResult transactionpb.SubmitTransactionResponse_Result
	sig, stat, err := s.sc.SubmitTransaction(txn, solanautil.CommitmentFromProto(req.Commitment))
	if err != nil {
		log.WithError(err).Warn("unhandled SubmitTransaction")
		return nil, status.Errorf(codes.Internal, "unhandled error from SubmitTransaction: %v", err)
	}
	if stat.ErrorResult != nil {
		// If it's a duplicate signature, we still want to process the Write()
		// in case that's what failed on an earlier call.
		if stat.ErrorResult.ErrorKey() == solana.TransactionErrorDuplicateSignature {
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

			txError, err := solanautil.MapTransactionError(*stat.ErrorResult)
			if err != nil {
				log.WithError(err).Warn("failed to map transaction error")
				resp.TransactionError = &commonpb.TransactionError{
					Reason: commonpb.TransactionError_UNKNOWN,
				}
			} else {
				resp.TransactionError = txError
			}

			return resp, nil
		}
	}

	entry := &model.Entry{
		Version: model.KinVersion_KIN4,
		Kind: &model.Entry_Solana{
			Solana: &model.SolanaEntry{
				Slot:        stat.Slot,
				Transaction: txn.Marshal(),
			},
		},
	}

	if err := s.history.Write(ctx, entry); err != nil {
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

	return &transactionpb.SubmitTransactionResponse{
		Result: submitResult,
		Signature: &commonpb.TransactionSignature{
			Value: sig[:],
		},
	}, nil
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
