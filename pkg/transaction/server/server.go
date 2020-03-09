package server

import (
	"bytes"
	"context"
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

	commonpb "github.com/kinecosystem/kin-api/genproto/common/v3"
	transactionpb "github.com/kinecosystem/kin-api/genproto/transaction/v3"

	"github.com/kinecosystem/agora-transaction-services/pkg/appindex"
	"github.com/kinecosystem/agora-transaction-services/pkg/data"
)

type server struct {
	log *logrus.Entry

	txStore  data.Store
	resolver appindex.Resolver

	client   horizon.ClientInterface
	clientV2 horizonclient.ClientInterface
}

// New returns a new transactionpb.TransactionServer.
func New(
	txStore data.Store,
	resolver appindex.Resolver,
	client horizon.ClientInterface,
	clientV2 horizonclient.ClientInterface,
) transactionpb.TransactionServer {
	return &server{
		log: logrus.StandardLogger().WithField("type", "transactionpb/server"),

		txStore:  txStore,
		resolver: resolver,

		client:   client,
		clientV2: clientV2,
	}
}

// SubmitSend implements transactionpb.TransactionServer.SubmitSpend.
func (s *server) SubmitSend(ctx context.Context, req *transactionpb.SubmitSendRequest) (*transactionpb.SubmitSendResponse, error) {
	log := s.log.WithField("method", "SubmitSend")
	if err := req.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, "")
	}

	var tx xdr.Transaction
	if _, err := xdr.Unmarshal(bytes.NewBuffer(req.TransactionXdr), &tx); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid xdr")
	}

	// todo: memo verification

	// todo: whitelisting
	// todo: timeout on txn send?
	resp, err := s.client.SubmitTransaction(base64.StdEncoding.EncodeToString(req.TransactionXdr))
	if err != nil {
		if hErr, ok := err.(*horizon.Error); ok {
			log.WithField("problem", hErr.Problem).Warn("Failed to submti txn")
		}

		// todo: proper inspection and error handling
		log.WithError(err).Warn("Failed to submit txn")
		return nil, status.Error(codes.Internal, "failed to submit transactionpb")
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
	if err := req.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, "")
	}

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
				log.Warn("Unexpected error from horizon:", hErr.Problem)
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
	memo, err := kin.MemoFromXDRString(tx.Hash, true)
	if err != nil {
		// This simply means it wasn't a valid agora memo, so we just
		// don't include any agora data.
		return resp, nil
	}

	url, err := s.resolver.Resolve(ctx, memo)
	if err == nil {
		resp.Item.AgoraDataUrl = url
	} else if err != appindex.ErrNotFound {
		return nil, status.Error(codes.Internal, "failed to resolve agora memo")
	}

	txData, err := s.txStore.Get(ctx, memo.ForeignKey())
	if err == nil {
		resp.Item.AgoraData = txData
	} else if err != data.ErrNotFound {
		return nil, status.Error(codes.Internal, "failed to retrieve agora data")
	}

	return resp, nil
}

// GetHistory implements transactionpb.TransactionServer.GetHistory.
func (s *server) GetHistory(ctx context.Context, req *transactionpb.GetHistoryRequest) (*transactionpb.GetHistoryResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, "")
	}

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
				log.Warn("Unexpected error from horizon:", hErr.Problem)
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
		memo, err := kin.MemoFromXDRString(tx.Hash, true)
		if err != nil {
			continue
		}

		url, err := s.resolver.Resolve(ctx, memo)
		switch err {
		case nil:
			item.AgoraDataUrl = url
		case appindex.ErrNotFound:
			continue
		default:
			return nil, status.Error(codes.Internal, "failed to retrieve agora data")
		}

		txData, err := s.txStore.Get(context.Background(), memo.ForeignKey())
		switch err {
		case nil:
			item.AgoraData = txData
		case data.ErrNotFound:
			return nil, status.Error(codes.Internal, "failed to retrieve agora data")
		default:
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
