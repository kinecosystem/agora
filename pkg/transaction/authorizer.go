package transaction

import (
	"bytes"
	"context"
	"strings"

	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/agora-common/kin/version"
	"github.com/kinecosystem/agora-common/webhook/signtransaction"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"

	"github.com/kinecosystem/agora/pkg/app"
	"github.com/kinecosystem/agora/pkg/invoice"
	"github.com/kinecosystem/agora/pkg/webhook"
)

// Authorizer authorizes transactions.
type Authorizer interface {
	// Authorize authorizes the provided transaction.
	//
	// Callers must perform actual submission, and any persistence
	// related to the transaction, such as invoice storing.
	Authorize(context.Context, Transaction) (Authorization, error)
}

type authorizer struct {
	log         *logrus.Entry
	mapper      app.Mapper
	configStore app.ConfigStore

	webhookClient *webhook.Client
	limiter       *Limiter
}

// NewAuthorizer returns an authorizer.
func NewAuthorizer(
	mapper app.Mapper,
	configStore app.ConfigStore,
	webhookClient *webhook.Client,
	limiter *Limiter,
) (Authorizer, error) {
	if err := registerMetrics(); err != nil {
		return nil, err
	}

	return &authorizer{
		log:           logrus.StandardLogger().WithField("type", "transaction/authorizer"),
		mapper:        mapper,
		configStore:   configStore,
		webhookClient: webhookClient,
		limiter:       limiter,
	}, nil
}

type Memo struct {
	Memo *kin.Memo
	Text *string
}

type Transaction struct {
	Version     version.KinVersion
	ID          []byte
	Memo        Memo
	OpCount     int
	InvoiceList *commonpb.InvoiceList
	SignRequest *signtransaction.Request
}

type AuthorizationResult int

const (
	AuthorizationResultOK = iota
	AuthorizationResultRejected
	AuthorizationResultInvoiceError
)

type Authorization struct {
	Result        int
	InvoiceErrors []*commonpb.InvoiceError
	SignResponse  *signtransaction.SuccessResponse
}

// Authorize implements Authorizer.Authorize.
func (s *authorizer) Authorize(ctx context.Context, txn Transaction) (a Authorization, err error) {
	log := s.log.WithField("method", "authorize")

	// The only way a transaction can be an earn is if it's using the binary memo
	// format, with a transaction type Earn. Otherwise, all transactions are considered
	// "something else".
	//
	// Note: we can't differentiate earns from spends using text memos.
	var isEarn bool
	var appIndex uint16
	if txn.Memo.Memo != nil {
		appIndex = txn.Memo.Memo.AppIndex()
		isEarn = txn.Memo.Memo.TransactionType() == kin.TransactionTypeEarn

		if txn.InvoiceList != nil {
			if len(txn.InvoiceList.Invoices) != txn.OpCount {
				return a, status.Error(codes.InvalidArgument, "invoice list size does not match op/instruction count")
			}

			expectedFK, err := invoice.GetSHA224Hash(txn.InvoiceList)
			if err != nil {
				log.WithError(err).Warn("failed to get invoice list hash")
				return a, status.Error(codes.Internal, "failed to validate invoice list hash")
			}

			fk := txn.Memo.Memo.ForeignKey()
			if !(bytes.Equal(fk[:28], expectedFK)) || fk[28] != byte(0) {
				return a, status.Error(codes.InvalidArgument, "invalid memo: fk did not match invoice list hash")
			}
		}
	} else if txn.InvoiceList != nil {
		return a, status.Error(codes.InvalidArgument, "transaction must contain valid kin binary memo to use invoices")
	} else if txn.Memo.Text != nil {
		if appID, ok := AppIDFromTextMemo(*txn.Memo.Text); ok {
			appIndex, err = s.mapper.GetAppIndex(ctx, appID)
			if err != nil && err != app.ErrMappingNotFound {
				log.WithError(err).Warn("failed to get app id mapping")
				return a, status.Error(codes.Internal, "failed to get app id mapping")
			}
		}
	}

	allowed, err := s.limiter.Allow(int(appIndex))
	if err != nil {
		log.WithError(err).Warn("failed to check rate limit")
	} else if !allowed {
		return a, status.Error(codes.ResourceExhausted, "rate limiter")
	}

	if appIndex > 0 {
		config, err := s.configStore.Get(ctx, appIndex)
		if err == app.ErrNotFound {
			return a, status.Error(codes.InvalidArgument, "app index not found")
		}
		if err != nil {
			log.WithError(err).Warn("failed to get app config")
			return a, status.Error(codes.Internal, "failed to get app config")
		}

		log = log.WithField("appIndex", appIndex)

		if !isEarn && config.SignTransactionURL != nil {
			log = log.WithField("url", *config.SignTransactionURL)

			a.SignResponse, err = s.webhookClient.SignTransaction(ctx, *config.SignTransactionURL, config.WebhookSecret, txn.SignRequest)
			if err != nil {
				if signTxErr, ok := err.(*webhook.SignTransactionError); ok {
					log = log.WithField("status", signTxErr.StatusCode)
					switch signTxErr.StatusCode {
					case 403:
						if len(signTxErr.OperationErrors) == 0 || len(txn.InvoiceList.GetInvoices()) == 0 {
							a.Result = AuthorizationResultRejected
							return a, nil
						}

						a.Result = AuthorizationResultInvoiceError
						a.InvoiceErrors = make([]*commonpb.InvoiceError, len(signTxErr.OperationErrors))
						for i, opErr := range signTxErr.OperationErrors {
							var reason commonpb.InvoiceError_Reason
							switch opErr.Reason {
							case signtransaction.AlreadyPaid:
								reason = commonpb.InvoiceError_ALREADY_PAID
							case signtransaction.WrongDestination:
								reason = commonpb.InvoiceError_WRONG_DESTINATION
							case signtransaction.SKUNotFound:
								reason = commonpb.InvoiceError_SKU_NOT_FOUND
							default:
								reason = commonpb.InvoiceError_UNKNOWN
							}

							if int(opErr.OperationIndex) >= len(txn.InvoiceList.GetInvoices()) {
								log.WithFields(logrus.Fields{
									"index":  opErr.OperationIndex,
									"reason": reason,
								}).Info("out of range index error, ignoring")
								continue
							}

							a.InvoiceErrors[i] = &commonpb.InvoiceError{
								OpIndex: opErr.OperationIndex,
								Invoice: txn.InvoiceList.Invoices[opErr.OperationIndex],
								Reason:  reason,
							}
						}
						return a, nil
					default:
						log.WithError(signTxErr).Warn("Received unexpected error from app server")
						return a, status.Error(codes.Internal, "failed to verify transaction with webhook")
					}
				}

				log.WithError(err).Warn("failed to call sign transaction webhook")
				return a, status.Error(codes.Internal, "failed to verify transaction with webhook")
			}
		}
	}

	return a, nil
}

// AppIDFromTextMemo returns the canonical string AppID given a memo string.
//
// If the provided memo is in the incorrect format, ok will be false.
func AppIDFromTextMemo(memo string) (appID string, ok bool) {
	parts := strings.Split(memo, "-")
	if len(parts) < 2 {
		return "", false
	}

	// Only one supported version of text memos exist
	if parts[0] != "1" {
		return "", false
	}

	// App IDs are expected to be 3 or 4 characters
	if !app.IsValidAppID(parts[1]) {
		return "", false
	}

	return parts[1], true
}
