package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-redis/redis_rate/v8"
	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/go/build"
	"github.com/kinecosystem/go/clients/horizon"
	"github.com/kinecosystem/go/network"
	"github.com/kinecosystem/go/xdr"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go/clients/horizonclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"
	transactionpb "github.com/kinecosystem/agora-api/genproto/transaction/v3"

	"github.com/kinecosystem/agora/pkg/app"
	"github.com/kinecosystem/agora/pkg/invoice"
	"github.com/kinecosystem/agora/pkg/transaction/history"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
	"github.com/kinecosystem/agora/pkg/webhook"
	"github.com/kinecosystem/agora/pkg/webhook/signtransaction"
)

const (
	globalRateLimitKey    = "submit-transaction-rate-limit-global"
	appRateLimitKeyFormat = "submit-transaction-rate-limit-app-%d"
)

type server struct {
	log              *logrus.Entry
	network          build.Network

	appConfigStore app.ConfigStore
	appMapper app.Mapper
	invoiceStore   invoice.Store
	loader         *historyLoader

	client        horizon.ClientInterface
	clientV2      horizonclient.ClientInterface
	webhookClient *webhook.Client

	limiter *redis_rate.Limiter
	config  *Config
}

type Config struct {
	// SubmitTxGlobalLimit is the number of SubmitTransaction requests allowed globally per second.
	//
	// A value <= 0 indicates that no rate limit is to be applied.
	SubmitTxGlobalLimit int

	// SubmitTxAppLimit is the number of SubmitTransaction requests allowed per app per second.
	//
	// A value <= 0 indicates that no rate limit is to be applied.
	SubmitTxAppLimit int
}

// New returns a new transactionpb.TransactionServer.
func New(
	appConfigStore app.ConfigStore,
	appMapper app.Mapper,
	invoiceStore invoice.Store,
	reader history.Reader,
	committer ingestion.Committer,
	client horizon.ClientInterface,
	clientV2 horizonclient.ClientInterface,
	webhookClient *webhook.Client,
	limiter *redis_rate.Limiter,
	config *Config,
) (transactionpb.TransactionServer, error) {
	network, err := kin.GetNetwork()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get network")
	}

	return &server{
		log:              logrus.StandardLogger().WithField("type", "transaction/server"),
		network:          network,

		appConfigStore: appConfigStore,
		appMapper: appMapper,
		invoiceStore:   invoiceStore,
		loader:         newLoader(client, reader, committer),

		client:        client,
		clientV2:      clientV2,
		webhookClient: webhookClient,

		limiter: limiter,
		config:  config,
	}, nil
}

// SubmitTransaction implements transactionpb.TransactionServer.SubmitTransaction.
func (s *server) SubmitTransaction(ctx context.Context, req *transactionpb.SubmitTransactionRequest) (*transactionpb.SubmitTransactionResponse, error) {
	log := s.log.WithField("method", "SubmitTransaction")

	if s.config.SubmitTxGlobalLimit > 0 {
		result, err := s.limiter.Allow(globalRateLimitKey, redis_rate.PerSecond(s.config.SubmitTxGlobalLimit))
		if err != nil {
			log.WithError(err).Warn("failed to check global rate limit")
		} else if !result.Allowed {
			return nil, status.Error(codes.Unavailable, "rate limited")
		}
	}

	e := &xdr.TransactionEnvelope{}
	if _, err := xdr.Unmarshal(bytes.NewBuffer(req.EnvelopeXdr), e); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid xdr")
	}

	if len(e.Signatures) == 0 {
		return nil, status.Error(codes.InvalidArgument, "missing transaction signature")
	}

	var err error
	var appIndex uint16
	var memo kin.Memo
	if e.Tx.Memo.Hash != nil && kin.IsValidMemoStrict(kin.Memo(*e.Tx.Memo.Hash)) {
		memo = kin.Memo(*e.Tx.Memo.Hash)
		appIndex = memo.AppIndex()

		if s.config.SubmitTxAppLimit > 0 {
			result, err := s.limiter.Allow(fmt.Sprintf(appRateLimitKeyFormat, memo.AppIndex()), redis_rate.PerSecond(s.config.SubmitTxAppLimit))
			if err != nil {
				log.WithError(err).Warn("failed to check per app rate limit")
			} else if !result.Allowed {
				return nil, status.Error(codes.Unavailable, "rate limited")
			}
		}

		if req.InvoiceList != nil {
			if len(req.InvoiceList.Invoices) != len(e.Tx.Operations) {
				return nil, status.Error(codes.InvalidArgument, "invoice list size does not match op count")
			}

			expectedFK, err := invoice.GetSHA224Hash(req.InvoiceList)
			if err != nil {
				log.WithError(err).Warn("failed to get invoice list hash")
				return nil, status.Error(codes.Internal, "failed to validate invoice list hash")
			}

			fk := memo.ForeignKey()
			if !(bytes.Equal(fk[:28], expectedFK)) || fk[28] != byte(0) {
				return nil, status.Error(codes.InvalidArgument, "invalid memo: fk did not match invoice list hash")
			}
		}
	} else if e.Tx.Memo.Text != nil {
		appID := parseAppID(*e.Tx.Memo.Text)
		if len(appID) != 0 {
			appIndex, err = s.appMapper.GetAppIndex(ctx, appID)
			if err != nil && err != app.ErrMappingNotFound {
				log.WithError(err).Warn("failed to get app id mapping")
				return nil, status.Error(codes.Internal, "failed to get app id mapping")
			}
		}
	}

	encodedXDR := base64.StdEncoding.EncodeToString(req.EnvelopeXdr)
	if appIndex > 0 {
		config, err := s.appConfigStore.Get(ctx, appIndex)
		if err == app.ErrNotFound {
			return nil, status.Error(codes.InvalidArgument, "app index not found")
		}
		if err != nil {
			log.WithError(err).Warn("failed to get app config")
			return nil, status.Error(codes.Internal, "failed to get app config")
		}

		log.WithField("appIndex", appIndex)

		reqBody, err := signtransaction.RequestBodyFromProto(req)
		if err != nil {
			log.WithError(err).Warn("failed to convert request for signing")
			return nil, status.Error(codes.Internal, "failed to submit transaction")
		}

		if config.SignTransactionURL != nil {
			encodedXDR, e, err = s.webhookClient.SignTransaction(ctx, *config.SignTransactionURL, config.WebhookSecret, reqBody)
			if err != nil {
				if signTxErr, ok := err.(*webhook.SignTransactionError); ok {
					switch signTxErr.StatusCode {
					case 400:
						log.WithError(signTxErr).Warn("Received 400 from app server")
						return nil, status.Error(codes.Internal, "failed to submit transaction")
					case 403:
						if len(signTxErr.OperationErrors) == 0 || len(req.InvoiceList.GetInvoices()) == 0 {
							return &transactionpb.SubmitTransactionResponse{
								Result: transactionpb.SubmitTransactionResponse_REJECTED,
							}, nil
						}

						invoiceErrs := make([]*transactionpb.SubmitTransactionResponse_InvoiceError, len(signTxErr.OperationErrors))
						for i, opErr := range signTxErr.OperationErrors {
							var reason transactionpb.SubmitTransactionResponse_InvoiceError_Reason
							switch opErr.Reason {
							case signtransaction.AlreadyPaid:
								reason = transactionpb.SubmitTransactionResponse_InvoiceError_ALREADY_PAID
							case signtransaction.WrongDestination:
								reason = transactionpb.SubmitTransactionResponse_InvoiceError_WRONG_DESTINATION
							case signtransaction.SKUNotFound:
								reason = transactionpb.SubmitTransactionResponse_InvoiceError_SKU_NOT_FOUND
							default:
								reason = transactionpb.SubmitTransactionResponse_InvoiceError_UNKNOWN
							}

							if int(opErr.OperationIndex) >= len(req.InvoiceList.GetInvoices()) {
								log.WithFields(logrus.Fields{
									"index":  opErr.OperationIndex,
									"reason": reason,
								}).Info("out of range index error, ignoring")
								continue
							}

							invoiceErrs[i] = &transactionpb.SubmitTransactionResponse_InvoiceError{
								OpIndex: opErr.OperationIndex,
								Invoice: req.InvoiceList.Invoices[opErr.OperationIndex],
								Reason:  reason,
							}
						}
						return &transactionpb.SubmitTransactionResponse{
							Result:        transactionpb.SubmitTransactionResponse_INVOICE_ERROR,
							InvoiceErrors: invoiceErrs,
						}, nil
					default:
						log.WithError(signTxErr).Warnf("Received %d from app server", signTxErr.StatusCode)
						return nil, status.Error(codes.Internal, "failed to submit transaction")
					}
				}

				log.WithError(err).Warn("failed to call sign transaction webhook")
				return nil, status.Error(codes.Internal, "failed to whitelist transaction")
			}
		}

		if config.InvoicingEnabled && req.InvoiceList != nil {
			txHash, err := network.HashTransaction(&e.Tx, s.network.Passphrase)
			if err != nil {
				return nil, status.Error(codes.Internal, "failed to hash transaction")
			}

			err = s.invoiceStore.Put(ctx, txHash[:], req.InvoiceList)
			if err != nil && err != invoice.ErrExists {
				log.WithError(err).Warn("failed to store invoice")
				return nil, status.Error(codes.Internal, "failed to store invoice")
			}
		}
	}

	// todo: timeout on txn send?
	resp, err := s.client.SubmitTransaction(encodedXDR)
	if err != nil {
		if hErr, ok := err.(*horizon.Error); ok {
			log.WithField("problem", hErr.Problem).Warn("Failed to submit txn")

			var encodedResultXDR string
			if err := json.Unmarshal(hErr.Problem.Extras["result_xdr"], &encodedResultXDR); err != nil {
				log.WithError(err).WithField("result_xdr", hErr.Problem.Extras["result_xdr"]).Warn("failed to unmarshal result XDR")
				return nil, status.Error(codes.Internal, "invalid json result encoding from horizon")
			}

			resultXDR, err := base64.StdEncoding.DecodeString(encodedResultXDR)
			if err != nil {
				log.WithError(err).WithField("result_xdr", encodedResultXDR).Warn("failed to decode result XDR")
				return nil, status.Error(codes.Internal, "invalid result encoding from horizon")
			}

			return &transactionpb.SubmitTransactionResponse{
				Result:    transactionpb.SubmitTransactionResponse_FAILED,
				ResultXdr: resultXDR,
			}, nil
		}

		log.WithError(err).Warn("Failed to submit txn")
		return nil, status.Error(codes.Internal, "failed to submit transaction")
	}

	hashBytes, err := hex.DecodeString(resp.Hash)
	if err != nil {
		return nil, status.Error(codes.Internal, "invalid hash encoding from horizon")
	}

	resultXDR, err := base64.StdEncoding.DecodeString(resp.Result)
	if err != nil {
		return nil, status.Error(codes.Internal, "invalid result encoding from horizon")
	}

	return &transactionpb.SubmitTransactionResponse{
		Hash: &commonpb.TransactionHash{
			Value: hashBytes,
		},
		Ledger:    int64(resp.Ledger),
		ResultXdr: resultXDR,
	}, nil
}

// GetTransaction implements transactionpb.TransactionServer.GetTransaction.
func (s *server) GetTransaction(ctx context.Context, req *transactionpb.GetTransactionRequest) (*transactionpb.GetTransactionResponse, error) {
	log := s.log.WithFields(logrus.Fields{
		"method": "GetTransaction",
		"hash":   hex.EncodeToString(req.TransactionHash.Value),
	})

	tx, err := s.loader.getTransaction(ctx, req.TransactionHash.Value)
	if err == history.ErrNotFound {
		return &transactionpb.GetTransactionResponse{State: transactionpb.GetTransactionResponse_UNKNOWN}, nil
	} else if err != nil {
		log.WithError(err).Warn("failed to get transaction")
		return nil, status.Error(codes.Internal, err.Error())
	}

	resp := &transactionpb.GetTransactionResponse{
		State:  transactionpb.GetTransactionResponse_SUCCESS,
		Ledger: tx.ledger,
		Item: &transactionpb.HistoryItem{
			Hash: &commonpb.TransactionHash{
				Value: tx.hash,
			},
			ResultXdr:   tx.resultXDR,
			EnvelopeXdr: tx.envelopeXDR,
			Cursor:      tx.cursor,
		},
	}

	var appIndex uint16
	if tx.memo != nil {
		appIndex = tx.memo.AppIndex()
	} else if len(tx.textMemo) > 0 {
		appID := parseAppID(tx.textMemo)
		if len(appID) != 0 {
			appIndex, err = s.appMapper.GetAppIndex(ctx, appID)
			if err != nil && err != app.ErrMappingNotFound {
				log.WithError(err).Warn("failed to get app id mapping")
				return nil, status.Error(codes.Internal, "failed to get app id mapping")
			}
		}
	}

	if appIndex == 0 {
		return resp, nil
	}

	// todo(caching): cache config
	appConfig, err := s.appConfigStore.Get(ctx, appIndex)
	if err != nil {
		if err == app.ErrNotFound {
			return resp, nil
		}

		log.WithError(err).Warn("Failed to get app config")
		return nil, status.Error(codes.Internal, "failed to retrieve agora data")
	}

	resp.Item.InvoiceList, err = s.getInvoiceList(ctx, appConfig, req.TransactionHash.Value)
	if err != nil {
		log.WithError(err).Warn("Failed to retrieve invoice list")
		return nil, status.Error(codes.Internal, "failed to retrieve invoice list")
	}

	return resp, nil
}

// GetHistory implements transactionpb.TransactionServer.GetHistory.
func (s *server) GetHistory(ctx context.Context, req *transactionpb.GetHistoryRequest) (*transactionpb.GetHistoryResponse, error) {
	log := s.log.WithFields(logrus.Fields{
		"method":  "GetHistory",
		"account": req.AccountId.Value,
	})

	txns, err := s.loader.getTransactions(ctx, req.AccountId.Value, req.Cursor, req.Direction)
	if err != nil {
		log.WithError(err).Warn("Failed to get history txns")
		return nil, status.Error(codes.Internal, "failed to get txns")
	}

	resp := &transactionpb.GetHistoryResponse{}

	// todo:  parallelize history lookups
	for _, tx := range txns {
		item := &transactionpb.HistoryItem{
			Hash: &commonpb.TransactionHash{
				Value: tx.hash,
			},
			ResultXdr:   tx.resultXDR,
			EnvelopeXdr: tx.envelopeXDR,
			Cursor:      tx.cursor,
		}

		// We append before filling out the rest of the data component
		// since it makes the control flow a lot simpler. We're adding the
		// item regardless (unless full error, in which case we're not returning
		// resp at all), so we can just continue if we decide to stop filling out
		// the data.
		//
		// Note: this only works because we're appending pointers.
		resp.Items = append(resp.Items, item)

		var appIndex uint16
		if tx.memo != nil {
			appIndex = tx.memo.AppIndex()
		} else if len(tx.textMemo) > 0 {
			appID := parseAppID(tx.textMemo)
			if len(appID) != 0 {
				appIndex, err = s.appMapper.GetAppIndex(ctx, appID)
				if err != nil && err != app.ErrMappingNotFound {
					log.WithError(err).Warn("failed to get app id mapping")
					return nil, status.Error(codes.Internal, "failed to get app id mapping")
				}
			}
		}

		if appIndex == 0 {
			continue
		}

		if appIndex > 0 {
			// todo(caching): cache config
			appConfig, err := s.appConfigStore.Get(ctx, appIndex)
			if err != nil {
				if err == app.ErrNotFound {
					return resp, nil
				}

				log.WithError(err).Warn("Failed to get app config")
				return nil, status.Error(codes.Internal, "failed to retrieve agora data")
			}

			item.InvoiceList, err = s.getInvoiceList(ctx, appConfig, tx.hash)
			if err != nil {
				log.WithError(err).Warn("Failed to retrieve invoice list")
				return nil, status.Error(codes.Internal, "failed to retrieve invoice list")
			}
		}
	}

	return resp, nil
}

func (s *server) getInvoiceList(ctx context.Context, appConfig *app.Config, txHash []byte) (*commonpb.InvoiceList, error) {
	// shortcut to avoid unnecessary lookups
	if !appConfig.InvoicingEnabled {
		return nil, nil
	}

	il, err := s.invoiceStore.Get(ctx, txHash)
	if err == invoice.ErrNotFound {
		return nil, nil
	}
	return il, err
}

func parseAppID(memo string) (appID string) {
	parts := strings.Split(memo, "-")
	if len(parts) < 2 {
		return ""
	}

	// Only one supported version of text memos exist
	if parts[0] != "1" {
		return ""
	}

	// App IDs are expected to be 3 or 4 characters
	if !app.IsValidAppID(parts[1]) {
		return ""
	}

	return parts[1]
}
