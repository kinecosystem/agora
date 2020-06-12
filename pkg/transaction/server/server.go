package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"

	"github.com/go-redis/redis_rate/v8"
	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/go/build"
	"github.com/kinecosystem/go/clients/horizon"
	"github.com/kinecosystem/go/keypair"
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
	"github.com/kinecosystem/agora/pkg/transaction"
	"github.com/kinecosystem/agora/pkg/webhook"
	"github.com/kinecosystem/agora/pkg/webhook/signtransaction"
)

const (
	globalRateLimitKey    = "submit-transaction-rate-limit-global"
	appRateLimitKeyFormat = "submit-transaction-rate-limit-app-%d"
)

type server struct {
	log              *logrus.Entry
	whitelistAccount *keypair.Full
	network          build.Network

	appConfigStore app.ConfigStore
	invoiceStore   invoice.Store

	client        horizon.ClientInterface
	clientV2      horizonclient.ClientInterface
	webhookClient *webhook.Client

	limiter *redis_rate.Limiter
	config  *Config
}

type Config struct {
	SubmitTxGlobalLimit int
	SubmitTxAppLimit    int
}

// New returns a new transactionpb.TransactionServer.
func New(
	whitelistAccount *keypair.Full,
	appConfigStore app.ConfigStore,
	invoiceStore invoice.Store,
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
		whitelistAccount: whitelistAccount,
		network:          network,

		appConfigStore: appConfigStore,
		invoiceStore:   invoiceStore,

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

	result, err := s.limiter.Allow(globalRateLimitKey, redis_rate.PerSecond(s.config.SubmitTxGlobalLimit))
	if err != nil {
		log.WithError(err).Warn("failed to check global rate limit")
	} else if !result.Allowed {
		return nil, status.Error(codes.Unavailable, "rate limited")
	}

	e := &xdr.TransactionEnvelope{}
	if _, err := xdr.Unmarshal(bytes.NewBuffer(req.EnvelopeXdr), e); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid xdr")
	}

	if len(e.Signatures) == 0 {
		return nil, status.Error(codes.InvalidArgument, "missing transaction signature")
	}

	encodedXDR := base64.StdEncoding.EncodeToString(req.EnvelopeXdr)
	if e.Tx.Memo.Hash != nil && kin.IsValidMemoStrict(kin.Memo(*e.Tx.Memo.Hash)) {
		memo := kin.Memo(*e.Tx.Memo.Hash)

		result, err := s.limiter.Allow(fmt.Sprintf(appRateLimitKeyFormat, memo.AppIndex()), redis_rate.PerSecond(s.config.SubmitTxAppLimit))
		if err != nil {
			log.WithError(err).Warn("failed to check per app rate limit")
		} else if !result.Allowed {
			return nil, status.Error(codes.Unavailable, "rate limited")
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

		// todo(testing): it's likely better to setup a 'test' webhook that
		//                will perform the whitelisting, rather than hack it in
		//                here.
		if memo.AppIndex() == 0 && s.whitelistAccount != nil {
			whitelisted, err := transaction.SignEnvelope(e, s.network, s.whitelistAccount.Seed())
			if err != nil {
				return nil, status.Error(codes.Internal, "failed to whitelist txn")
			}
			e = whitelisted

			envelopeBytes, err := e.MarshalBinary()
			if err != nil {
				return nil, status.Error(codes.Internal, "failed to marshal transaction envelope")
			}
			encodedXDR = base64.StdEncoding.EncodeToString(envelopeBytes)
		} else {
			config, err := s.appConfigStore.Get(ctx, memo.AppIndex())
			if err == app.ErrNotFound {
				return nil, status.Error(codes.InvalidArgument, "app index not found")
			}
			if err != nil {
				log.WithError(err).Warn("failed to get app config")
				return nil, status.Error(codes.Internal, "failed to get app config")
			}

			log.WithField("appIndex", memo.AppIndex())

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
		}

		if req.InvoiceList != nil {
			txBytes, err := e.MarshalBinary()
			if err != nil {
				return nil, status.Error(codes.Internal, "failed to marshal transaction")
			}

			txHash := sha256.Sum256(txBytes)
			err = s.invoiceStore.Put(ctx, txHash[:], req.InvoiceList)
			if err != nil && err != invoice.ErrExists {
			} else if err != nil {
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
		}

		// todo: proper inspection and error handling
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

	// todo: figure out the details of non-success states to properly populate the State.
	tx, err := s.client.LoadTransaction(hex.EncodeToString(req.TransactionHash.Value))
	if err != nil {
		if hErr, ok := err.(*horizon.Error); ok {
			switch hErr.Problem.Status {
			case http.StatusNotFound:
				return nil, status.Error(codes.NotFound, "")
			default:
				log.WithField("problem", hErr.Problem).Warn("Unexpected error from horizon")
			}
		}

		log.WithError(err).Warn("Unexpected error from horizon")
		return nil, status.Error(codes.Internal, err.Error())
	}

	_, result, envelope, err := getBinaryBlobs(tx.Hash, tx.ResultXdr, tx.EnvelopeXdr)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	resp := &transactionpb.GetTransactionResponse{
		State:  transactionpb.GetTransactionResponse_SUCCESS,
		Ledger: int64(tx.Ledger),
		Item: &transactionpb.HistoryItem{
			Hash:        req.TransactionHash,
			ResultXdr:   result,
			EnvelopeXdr: envelope,
			Cursor:      getCursor(tx.PT),
		},
	}

	// todo: configurable encoding strictness?
	//
	// If the memo was valid, and we found the corresponding data for
	// it, populate the response. Otherwise, unless it was an unexpected
	// failure, we simply don't include the data.
	memo, err := kin.MemoFromXDRString(tx.Memo, true)
	if err != nil {
		// This simply means it wasn't a valid agora memo, so we just
		// don't include any agora data.
		return resp, nil
	}

	// todo(caching): cache config
	appConfig, err := s.appConfigStore.Get(ctx, memo.AppIndex())
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

	txnReq := horizonclient.TransactionRequest{
		ForAccount:    req.AccountId.Value,
		IncludeFailed: false,
		Limit:         100, // todo: we may eventually want to reduce this
	}

	switch req.Direction {
	case transactionpb.GetHistoryRequest_ASC:
		txnReq.Order = horizonclient.OrderAsc
	case transactionpb.GetHistoryRequest_DESC:
		txnReq.Order = horizonclient.OrderDesc
	}

	if req.Cursor != nil {
		// todo: formalize an internal encoding?
		txnReq.Cursor = string(req.Cursor.Value)
	}

	txns, err := s.clientV2.Transactions(txnReq)
	if err != nil {
		if hErr, ok := err.(*horizonclient.Error); ok {
			switch hErr.Problem.Status {
			case http.StatusNotFound:
				return nil, status.Error(codes.NotFound, "")
			default:
				log.WithField("problem", hErr.Problem).Warn("Unexpected error from horizon")
			}
		}

		log.WithError(err).Warn("Failed to get history txns")
		return nil, status.Error(codes.Internal, "failed to get horizon txns")
	}

	resp := &transactionpb.GetHistoryResponse{}

	// todo:  parallelize history lookups
	for _, tx := range txns.Embedded.Records {
		hash, result, envelope, err := getBinaryBlobs(tx.Hash, tx.ResultXdr, tx.EnvelopeXdr)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}

		item := &transactionpb.HistoryItem{
			Hash: &commonpb.TransactionHash{
				Value: hash,
			},
			ResultXdr:   result,
			EnvelopeXdr: envelope,
			Cursor:      getCursor(tx.PT),
		}

		// We append before filling out the rest of the data component
		// since it makes the control flow a lot simpler. We're adding the
		// item regardless (unless full error, in which case we're not returning
		// resp at all), so we can just continue if we decide to stop filling out
		// the data.
		//
		// Note: this only works because we're appending pointers.
		resp.Items = append(resp.Items, item)

		// todo: configurable encoding strictness?
		//
		// If the memo was valid, and we found the corresponding data for
		// it, populate the response. Otherwise, unless it was an unexpected
		// failure, we simply don't include the data.
		memo, err := kin.MemoFromXDRString(tx.Memo, true)
		if err != nil {
			continue
		}

		// todo(caching): cache config
		appConfig, err := s.appConfigStore.Get(ctx, memo.AppIndex())
		if err != nil {
			if err == app.ErrNotFound {
				return resp, nil
			}

			log.WithError(err).Warn("Failed to get app config")
			return nil, status.Error(codes.Internal, "failed to retrieve agora data")
		}

		item.InvoiceList, err = s.getInvoiceList(ctx, appConfig, hash)
		if err != nil {
			log.WithError(err).Warn("Failed to retrieve invoice list")
			return nil, status.Error(codes.Internal, "failed to retrieve invoice list")
		}
	}

	return resp, nil
}

func getBinaryBlobs(hash, result, envelope string) (hashBytes, resultBytes, envelopeBytes []byte, err error) {
	hashBytes, err = hex.DecodeString(hash)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to decode hash")
	}

	resultBytes, err = base64.StdEncoding.DecodeString(result)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to decode xdr")
	}

	envelopeBytes, err = base64.StdEncoding.DecodeString(envelope)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to decode envelope")
	}

	return hashBytes, resultBytes, envelopeBytes, nil
}

func getCursor(c string) *transactionpb.Cursor {
	if c == "" {
		return nil
	}

	// todo: it may be better to wrap the token, or something.
	return &transactionpb.Cursor{
		Value: []byte(c),
	}
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
