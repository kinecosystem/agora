package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/go/clients/horizon"
	"github.com/kinecosystem/go/xdr"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go/clients/horizonclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/kinecosystem/kin-api/genproto/common/v3"
	transactionpb "github.com/kinecosystem/kin-api/genproto/transaction/v3"

	"github.com/kinecosystem/agora-transaction-services-internal/pkg/app"
	"github.com/kinecosystem/agora-transaction-services-internal/pkg/invoice"
)

type server struct {
	log *logrus.Entry

	appConfigStore app.ConfigStore
	invoiceStore   invoice.Store

	client   horizon.ClientInterface
	clientV2 horizonclient.ClientInterface
}

// New returns a new transactionpb.TransactionServer.
func New(
	appConfigStore app.ConfigStore,
	invoiceStore invoice.Store,
	client horizon.ClientInterface,
	clientV2 horizonclient.ClientInterface,
) transactionpb.TransactionServer {
	return &server{
		log: logrus.StandardLogger().WithField("type", "transaction/server"),

		appConfigStore: appConfigStore,
		invoiceStore:   invoiceStore,

		client:   client,
		clientV2: clientV2,
	}
}

// SubmitSend implements transactionpb.TransactionServer.SubmitSpend.
func (s *server) SubmitSend(ctx context.Context, req *transactionpb.SubmitSendRequest) (*transactionpb.SubmitSendResponse, error) {
	log := s.log.WithField("method", "SubmitSend")

	var tx xdr.Transaction
	if _, err := xdr.Unmarshal(bytes.NewBuffer(req.TransactionXdr), &tx); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid xdr")
	}

	// todo: memo verification even if invoice is nil
	if req.Invoice != nil {
		genTime, err := ptypes.Timestamp(req.Invoice.Nonce.GenerationTime)
		if err != nil {
			log.WithError(err).Warn("failed to convert nonce generation time")
			return nil, status.Error(codes.Internal, "failed to validate invoice")
		}
		t := time.Now()
		if genTime.After(t.Add(1*time.Hour)) || genTime.Before(t.Add(-24*time.Hour)) {
			return &transactionpb.SubmitSendResponse{Result: transactionpb.SubmitSendResponse_INVALID_INVOICE_NONCE}, nil
		}

		if tx.Memo.Hash == nil {
			return nil, status.Error(codes.InvalidArgument, "invalid memo")
		}

		memo := kin.Memo(*tx.Memo.Hash)
		if !kin.IsValidMemoStrict(memo) {
			return nil, status.Error(codes.InvalidArgument, "invalid memo")
		}

		expectedFK, err := invoice.GetHash(req.Invoice)
		if err != nil {
			log.WithError(err).Warn("failed to get invoice hash")
			return nil, status.Error(codes.Internal, "failed to validate invoice")
		}

		fk := memo.ForeignKey()
		if !(bytes.Equal(fk[:28], expectedFK)) || fk[28] != byte(0) {
			return nil, status.Error(codes.InvalidArgument, "invalid memo: fk did not match invoice hash")
		}

		txBytes, err := tx.MarshalBinary()
		if err != nil {
			return nil, status.Error(codes.Internal, "failed to marshal transaction")
		}

		txHash := sha256.Sum256(txBytes)
		err = s.invoiceStore.Add(ctx, req.Invoice, txHash[:])
		if err != nil {
			if err == invoice.ErrExists {
				return &transactionpb.SubmitSendResponse{Result: transactionpb.SubmitSendResponse_INVALID_INVOICE_NONCE}, nil
			}

			log.WithError(err).Warn("failed to store invoice")
			return nil, status.Error(codes.Internal, "failed to store invoice")
		}
	}

	// todo: timeout on txn send?
	resp, err := s.client.SubmitTransaction(base64.StdEncoding.EncodeToString(req.TransactionXdr))
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

	return &transactionpb.SubmitSendResponse{
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

	url, err := appConfig.GetAgoraDataURL(memo)
	if err == nil {
		resp.Item.AgoraDataUrl = url
	} else if err != app.ErrURLNotSet {
		log.WithError(err).Warn("Failed to get agora data url")
		return nil, status.Error(codes.Internal, "failed to retrieve agora data")
	}

	agoraData, err := s.resolveMemoFK(ctx, appConfig, memo)
	if err != nil {
		return nil, err
	}

	if agoraData != nil {
		resp.Item.AgoraData = agoraData
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

		url, err := appConfig.GetAgoraDataURL(memo)
		if err == nil {
			item.AgoraDataUrl = url
		} else if err != app.ErrURLNotSet {
			log.WithError(err).Warn("Failed to get agora data url")
			return nil, status.Error(codes.Internal, "failed to retrieve agora data")
		}

		agoraData, err := s.resolveMemoFK(ctx, appConfig, memo)
		if err != nil {
			return nil, err
		}

		if agoraData != nil {
			item.AgoraData = agoraData
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

func (s *server) resolveMemoFK(ctx context.Context, appConfig *app.Config, memo kin.Memo) (*commonpb.AgoraData, error) {
	// TODO: attempt to resolve using 3p service/cache

	if !appConfig.InvoicingEnabled {
		return nil, nil
	}

	fk := memo.ForeignKey()
	if fk[28] != 0 {
		return nil, nil
	}

	invoiceHash := fk[:28]
	record, err := s.invoiceStore.Get(ctx, invoiceHash)
	if err != nil {
		if err == invoice.ErrNotFound {
			return nil, nil
		}
		return nil, status.Error(codes.Internal, "failed to resolve agora data")
	}

	total := int64(0)
	for _, item := range record.Invoice.Items {
		total += item.Amount
	}

	return &commonpb.AgoraData{
		Title:           appConfig.AppName,
		Description:     fmt.Sprintf("# of line items: %d", len(record.Invoice.Items)),
		TransactionType: commonpb.AgoraData_SPEND, // TODO: verify if earn/spend if/when both use invoices
		TotalAmount:     total,
		ForeignKey:      invoiceHash,
		Invoice:         record.Invoice,
	}, nil
}
