package transaction

import (
	"bytes"
	"context"
	"crypto/ed25519"

	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/webhook/signtransaction"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"

	"github.com/kinecosystem/agora/pkg/app"
	"github.com/kinecosystem/agora/pkg/webhook"
)

// Authorizer authorizes transactions.
type Authorizer interface {
	// Authorize authorizes the provided transaction.
	//
	// Callers must perform actual submission, and any persistence
	// related to the transaction, such as invoice storing.
	Authorize(ctx context.Context, tx solana.Transaction, il *commonpb.InvoiceList, ignoreSigned bool) (Authorization, error)
}

type authorizer struct {
	log           *logrus.Entry
	mapper        app.Mapper
	configStore   app.ConfigStore
	mint          ed25519.PublicKey
	subsidizer    ed25519.PublicKey
	subsidizerKey ed25519.PrivateKey
	minLamports   uint64

	webhookClient *webhook.Client
	limiter       *Limiter
}

// NewAuthorizer returns an authorizer.
func NewAuthorizer(
	mapper app.Mapper,
	configStore app.ConfigStore,
	webhookClient *webhook.Client,
	limiter *Limiter,
	subsidizer ed25519.PrivateKey,
	mint ed25519.PublicKey,
	minLamports uint64,
) (Authorizer, error) {
	if err := registerMetrics(); err != nil {
		return nil, err
	}

	a := &authorizer{
		log:           logrus.StandardLogger().WithField("type", "transaction/authorizer"),
		mapper:        mapper,
		configStore:   configStore,
		webhookClient: webhookClient,
		limiter:       limiter,
		mint:          mint,
		minLamports:   minLamports,
	}

	if len(subsidizer) == ed25519.PrivateKeySize {
		a.subsidizerKey = subsidizer
		a.subsidizer = subsidizer.Public().(ed25519.PublicKey)
	}

	return a, nil
}

type Memo struct {
	Memo *kin.Memo
	Text *string
}

type AuthorizationResult int

const (
	AuthorizationResultOK = iota
	AuthorizationResultRejected
	AuthorizationResultInvoiceError
	AuthorizationResultPayerRequired
)

type Authorization struct {
	Result        int
	InvoiceErrors []*commonpb.InvoiceError
	SignResponse  *signtransaction.SuccessResponse
}

func (s *authorizer) Authorize(ctx context.Context, raw solana.Transaction, il *commonpb.InvoiceList, ignoreSigned bool) (a Authorization, err error) {
	log := s.log.WithField("method", "Authorize")

	tx, err := kin.ParseTransaction(raw, il)
	if err != nil {
		return a, status.Errorf(codes.InvalidArgument, "invalid transaction: %s", err.Error())
	}

	//
	// Validate AppIndex / AppID.
	//
	appIndex := tx.AppIndex
	if tx.AppID != "" {
		appIndex, err = s.mapper.GetAppIndex(ctx, tx.AppID)
		if err == nil && tx.AppIndex > 0 && tx.AppIndex != appIndex {
			return a, status.Errorf(codes.InvalidArgument, "app id mapping does not match registered (got %d, expected %d)", tx.AppIndex, appIndex)
		} else if err == app.ErrMappingNotFound && tx.AppIndex > 0 {
			return a, status.Errorf(codes.InvalidArgument, "app id mapping is not registered (%s -> %d)", tx.AppID, tx.AppIndex)
		} else if err != nil && err != app.ErrMappingNotFound {
			log.WithError(err).Warn("failed to get app id mapping")
			return a, status.Error(codes.Internal, "failed to get app id mapping")
		}
	}
	if appIndex == 0 {
		appIndex, _ = app.GetAppIndex(ctx)
	}

	//
	// Validate transaction operations themselves
	//
	creations := make(map[string]struct{})
	transferDests := make(map[string]struct{})
	for r := range tx.Regions {
		for c := range tx.Regions[r].Creations {
			if tx.Regions[r].Creations[c].Create != nil {
				creations[string(tx.Regions[r].Creations[c].Create.Address)] = struct{}{}

				if tx.Regions[r].Creations[c].Create.Lamports != s.minLamports {
					return a, status.Error(codes.InvalidArgument, "create has incorrect amount of min lamports")
				}
				if !bytes.Equal(s.mint, tx.Regions[r].Creations[c].Initialize.Mint) {
					return a, status.Error(codes.InvalidArgument, "create for non-kin token")
				}
			} else if tx.Regions[r].Creations[c].CreateAssoc != nil {
				creations[string(tx.Regions[r].Creations[c].CreateAssoc.Address)] = struct{}{}

				if !bytes.Equal(s.mint, tx.Regions[r].Creations[c].CreateAssoc.Mint) {
					return a, status.Error(codes.InvalidArgument, "create for non-kin token")
				}
			} else {
				return a, status.Error(codes.InvalidArgument, "create without create instruction")
			}

			// Extra safety precautions
			if bytes.Equal(s.subsidizer, tx.Regions[r].Creations[c].CloseAuthority.CurrentAuthority) {
				return a, status.Error(codes.InvalidArgument, "create change close authority of a subsidizer owned account")
			}
			if tx.Regions[r].Creations[c].AccountHolder != nil {
				if bytes.Equal(s.subsidizer, tx.Regions[r].Creations[c].AccountHolder.CurrentAuthority) {
					return a, status.Error(codes.InvalidArgument, "cannot change account holder of a subsidizer owned account")
				}
			}
		}

		for t := range tx.Regions[r].Transfers {
			transferDests[string(tx.Regions[r].Transfers[t].Destination)] = struct{}{}
			if bytes.Equal(s.subsidizer, tx.Regions[r].Transfers[t].Owner) {
				return a, status.Error(codes.InvalidArgument, "cannot transfer from an account owned by subsidizer")
			}
		}
	}

	for createdAccount := range creations {
		if _, ok := transferDests[createdAccount]; !ok {
			return a, status.Error(codes.InvalidArgument, "created accounts must be a recipient of a transfer")
		}
	}

	if ignoreSigned && raw.Signatures[0] != (solana.Signature{}) {
		return Authorization{
			Result:        AuthorizationResultOK,
			InvoiceErrors: nil,
			SignResponse: &signtransaction.SuccessResponse{
				Signature: raw.Signature(),
			},
		}, nil
	}

	//
	// If the subsidizer is a service subsidizer, then we sign it before submitting
	// any SignRequests to provide a transaction signature to the webhooks.
	//
	var signed bool
	if bytes.Equal(s.subsidizer, raw.Message.Accounts[0]) {
		// note: we only do rate limiting on the service subsidizers behalf.
		allowed, err := s.limiter.Allow(int(appIndex))
		if err != nil {
			log.WithError(err).Warn("failed to check rate limit")
		} else if !allowed {
			return a, status.Error(codes.ResourceExhausted, "rate limited")
		}

		if err := raw.Sign(s.subsidizerKey); err != nil {
			return a, status.Error(codes.Internal, "failed to subsidize transaction")
		}

		signed = true
		a.SignResponse = &signtransaction.SuccessResponse{
			Signature: raw.Signature(),
		}
	}

	// If we don't have an app index, we can't proceed further, so we just exit early.
	if appIndex == 0 {
		if signed {
			return a, nil
		}

		return Authorization{
			Result: AuthorizationResultPayerRequired,
		}, nil
	}

	// If the transaction is signed, we still need to forward the SignTransaction request to the
	// webhook for approval (in cases where it is not an earn).
	var isEarn bool
	for r := range tx.Regions {
		if tx.Regions[r].Memo != nil && tx.Regions[r].Memo.TransactionType() == kin.TransactionTypeEarn {
			isEarn = true
			break
		}
	}

	if isEarn && signed {
		return a, nil
	}

	cfg, err := s.configStore.Get(ctx, appIndex)
	if err == app.ErrNotFound || cfg == nil || cfg.SignTransactionURL == nil {
		if signed {
			// Webhooks aren't mandatory for earns if the transaction is already signed.
			return a, nil
		}

		return Authorization{
			Result: AuthorizationResultPayerRequired,
		}, nil
	} else if err != nil {
		return a, status.Error(codes.Internal, "failed to get app config")
	}

	var ilBytes []byte
	if il != nil {
		ilBytes, err = proto.Marshal(il)
		if err != nil {
			return a, status.Error(codes.Internal, "failed to serialize invoice")
		}
	}

	req := &signtransaction.Request{
		KinVersion:        4,
		SolanaTransaction: raw.Marshal(),
		InvoiceList:       ilBytes,
	}
	signResponse, err := s.webhookClient.SignTransaction(ctx, *cfg.SignTransactionURL, cfg.WebhookSecret, req)
	if err != nil {
		if signTxErr, ok := err.(*webhook.SignTransactionError); ok {
			log = log.WithField("status", signTxErr.StatusCode)
			switch signTxErr.StatusCode {
			case 403:
				if len(signTxErr.OperationErrors) == 0 || len(il.GetInvoices()) == 0 {
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

					if int(opErr.OperationIndex) >= len(il.GetInvoices()) {
						log.WithFields(logrus.Fields{
							"index":  opErr.OperationIndex,
							"reason": reason,
						}).Info("out of range index error, ignoring")
						continue
					}

					a.InvoiceErrors[i] = &commonpb.InvoiceError{
						OpIndex: opErr.OperationIndex,
						Invoice: il.Invoices[opErr.OperationIndex],
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
	} else if a.SignResponse == nil {
		a.SignResponse = &signtransaction.SuccessResponse{}
	}

	if len(signResponse.Signature) > 0 {
		if len(signResponse.Signature) != ed25519.SignatureSize {
			return a, status.Error(codes.Internal, "webhook returned an invalid signature")
		}

		copy(raw.Signatures[0][:], signResponse.Signature)
		a.SignResponse = &signtransaction.SuccessResponse{
			Signature: raw.Signature(),
		}
	} else if len(a.SignResponse.Signature) == 0 {
		// If there is no signature at this point, it's likely an issue with the webhook.
		//
		// Either we should have signed the transaction, and have simply forwarded the request for approval,
		// or the webhook returned "ok" without a signature, which is an unexpected flow.
		return Authorization{
			Result: AuthorizationResultPayerRequired,
		}, nil
	}

	return a, nil
}
