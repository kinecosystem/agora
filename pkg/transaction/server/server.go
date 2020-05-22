package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net/http"

	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/go/clients/horizon"
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

// SubmitTransaction implements transactionpb.TransactionServer.SubmitTransaction.
func (s *server) SubmitTransaction(ctx context.Context, req *transactionpb.SubmitTransactionRequest) (*transactionpb.SubmitTransactionResponse, error) {
	log := s.log.WithField("method", "SubmitTransaction")

	var tx xdr.Transaction
	if _, err := xdr.Unmarshal(bytes.NewBuffer(req.TransactionXdr), &tx); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid xdr")
	}

	// todo: memo verification even if invoice is nil
	if req.InvoiceList != nil {
		if len(req.InvoiceList.Invoices) != len(tx.Operations) {
			return nil, status.Error(codes.InvalidArgument, "invoice list size does not match op count")
		}

		if tx.Memo.Hash == nil {
			return nil, status.Error(codes.InvalidArgument, "invalid memo")
		}

		memo := kin.Memo(*tx.Memo.Hash)
		if !kin.IsValidMemoStrict(memo) {
			return nil, status.Error(codes.InvalidArgument, "invalid memo")
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

		// todo(callback): verify with third party before.

		txBytes, err := tx.MarshalBinary()
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
	return il, nil
}
