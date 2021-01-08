package stellar

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"

	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/agora-common/kin/version"
	"github.com/kinecosystem/go/build"
	"github.com/kinecosystem/go/clients/horizon"
	kinnetwork "github.com/kinecosystem/go/network"
	"github.com/kinecosystem/go/xdr"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"
	transactionpb "github.com/kinecosystem/agora-api/genproto/transaction/v3"

	"github.com/kinecosystem/agora/pkg/app"
	"github.com/kinecosystem/agora/pkg/invoice"
	"github.com/kinecosystem/agora/pkg/transaction"
	"github.com/kinecosystem/agora/pkg/transaction/history"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
	"github.com/kinecosystem/agora/pkg/webhook/signtransaction"
)

var (
	submitTxCounter = transaction.SubmitTransactionCounter.WithLabelValues("3")
)

type server struct {
	log         *logrus.Entry
	network     build.Network
	kin2Network build.Network

	authorizer transaction.Authorizer

	appConfigStore app.ConfigStore
	appMapper      app.Mapper
	invoiceStore   invoice.Store
	loader         *historyLoader
	kin2Loader     *historyLoader

	client     horizon.ClientInterface
	kin2Client horizon.ClientInterface
}

// New returns a new transactionpb.TransactionServer.
func New(
	appConfigStore app.ConfigStore,
	appMapper app.Mapper,
	invoiceStore invoice.Store,
	reader history.Reader,
	committer ingestion.Committer,
	authorizer transaction.Authorizer,
	client horizon.ClientInterface,
	kin2Client horizon.ClientInterface,
) (transactionpb.TransactionServer, error) {
	network, err := kin.GetNetwork()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get network")
	}

	kin2Network, err := kin.GetKin2Network()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get kin 2 network")
	}

	return &server{
		log:         logrus.StandardLogger().WithField("type", "transaction/stellar/server"),
		network:     network,
		kin2Network: kin2Network,

		authorizer: authorizer,

		appConfigStore: appConfigStore,
		appMapper:      appMapper,
		invoiceStore:   invoiceStore,
		loader:         newLoader(client, reader, committer),
		kin2Loader:     newLoader(kin2Client, reader, committer),

		client:     client,
		kin2Client: kin2Client,
	}, nil
}

// SubmitTransaction implements transactionpb.TransactionServer.SubmitTransaction.
func (s *server) SubmitTransaction(ctx context.Context, req *transactionpb.SubmitTransactionRequest) (*transactionpb.SubmitTransactionResponse, error) {
	log := s.log.WithField("method", "SubmitTransaction")

	submitTxCounter.Inc()

	kinVersion, err := version.GetCtxKinVersion(ctx)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	var client horizon.ClientInterface
	var network build.Network
	switch kinVersion {
	case version.KinVersion2:
		client = s.kin2Client
		network = s.kin2Network
	default:
		client = s.client
		network = s.network
	}

	e := &xdr.TransactionEnvelope{}
	if _, err := xdr.Unmarshal(bytes.NewBuffer(req.EnvelopeXdr), e); err != nil {
		log.WithError(err).Debug("invalid xdr, dropping")
		return nil, status.Error(codes.InvalidArgument, "invalid xdr")
	}
	if len(e.Signatures) == 0 {
		return nil, status.Error(codes.InvalidArgument, "missing transaction signature")
	}

	//
	// Construct the transaction.Transaction
	//
	tx := transaction.Transaction{
		InvoiceList: req.InvoiceList,
		OpCount:     len(e.Tx.Operations),
	}

	rawHash, err := kinnetwork.HashTransaction(&e.Tx, network.Passphrase)
	if err != nil {
		log.WithError(err).Warn("failed to compute hash of submitted transaction")
		return nil, status.Error(codes.Internal, "failed to compute tx hash")
	}

	tx.ID = rawHash[:]
	if e.Tx.Memo.Hash != nil && kin.IsValidMemoStrict(kin.Memo(*e.Tx.Memo.Hash)) {
		m := kin.Memo(*e.Tx.Memo.Hash)
		tx.Memo.Memo = &m
	} else if e.Tx.Memo.Text != nil {
		tx.Memo.Text = e.Tx.Memo.Text
	}

	tx.SignRequest, err = signtransaction.CreateStellarRequest(kinVersion, req)
	if err != nil {
		log.WithError(err).Warn("failed to convert request for signing")
		return nil, status.Error(codes.Internal, "failed to submit transaction")
	}

	//
	// Authorize and run all preflight checks
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
			Hash: &commonpb.TransactionHash{
				Value: tx.ID,
			},
			InvoiceErrors: result.InvoiceErrors,
		}, nil
	case transaction.AuthorizationResultRejected:
		return &transactionpb.SubmitTransactionResponse{
			Result: transactionpb.SubmitTransactionResponse_REJECTED,
			Hash: &commonpb.TransactionHash{
				Value: tx.ID,
			},
		}, nil
	default:
		log.WithField("result", result.Result).Warn("unexpected authorization result")
		return nil, status.Error(codes.Internal, "unhandled authorization error")
	}

	var envelopeBytes []byte
	if result.SignResponse != nil {
		envelopeBytes = result.SignResponse.EnvelopeXDR

		// Ensure the returned XDR is valid.
		if err := e.UnmarshalBinary(result.SignResponse.EnvelopeXDR); err != nil {
			return nil, status.Error(codes.Internal, "webhook returned an invalid response")
		}
	} else {
		envelopeBytes, err = e.MarshalBinary()
		if err != nil {
			return nil, status.Error(codes.Internal, "failed to marshal transaction envelope")
		}
	}

	if tx.InvoiceList != nil {
		if err := s.invoiceStore.Put(ctx, tx.ID, tx.InvoiceList); err != nil && err != invoice.ErrExists {
			return nil, status.Errorf(codes.Internal, "failed to store invoice list")
		}
	}

	//
	// Perform actual submission
	//
	resp, err := client.SubmitTransaction(base64.StdEncoding.EncodeToString(envelopeBytes))
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
				Hash: &commonpb.TransactionHash{
					Value: tx.ID,
				},
				Result:    transactionpb.SubmitTransactionResponse_FAILED,
				ResultXdr: resultXDR,
			}, nil
		}

		log.WithError(err).Warn("Failed to submit txn")
		return nil, status.Error(codes.Internal, "failed to submit transaction")
	}

	resultXDR, err := base64.StdEncoding.DecodeString(resp.Result)
	if err != nil {
		return nil, status.Error(codes.Internal, "invalid result encoding from horizon")
	}

	return &transactionpb.SubmitTransactionResponse{
		Hash: &commonpb.TransactionHash{
			Value: tx.ID,
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

	kinVersion, err := version.GetCtxKinVersion(ctx)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	var loader *historyLoader
	switch kinVersion {
	case version.KinVersion2:
		loader = s.kin2Loader
	default:
		loader = s.loader
	}

	tx, err := loader.getTransaction(ctx, req.TransactionHash.Value)
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

	appIndex, err := s.appIndexFromTxData(ctx, tx)
	if err != nil && err != app.ErrMappingNotFound {
		log.WithError(err).Warn("failed to get app index")
		return nil, status.Error(codes.Internal, "failed to get app index")
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

	if tx.memo != nil {
		resp.Item.InvoiceList, err = s.getInvoiceList(ctx, appConfig, req.TransactionHash.Value)
		if err != nil {
			log.WithError(err).Warn("Failed to retrieve invoice list")
			return nil, status.Error(codes.Internal, "failed to retrieve invoice list")
		}
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

		appIndex, err := s.appIndexFromTxData(ctx, tx)
		if err != nil && err != app.ErrMappingNotFound {
			log.WithError(err).Warn("failed to get app index")
			return nil, status.Error(codes.Internal, "failed to get app index")
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

			if tx.memo != nil {
				item.InvoiceList, err = s.getInvoiceList(ctx, appConfig, tx.hash)
				if err != nil {
					log.WithError(err).Warn("Failed to retrieve invoice list")
					return nil, status.Error(codes.Internal, "failed to retrieve invoice list")
				}
			}
		}
	}

	return resp, nil
}

func (s *server) getInvoiceList(ctx context.Context, appConfig *app.Config, txHash []byte) (*commonpb.InvoiceList, error) {
	il, err := s.invoiceStore.Get(ctx, txHash)
	if err == invoice.ErrNotFound {
		return nil, nil
	}
	return il, err
}

func (s *server) appIndexFromTxData(ctx context.Context, tx txData) (appIndex uint16, err error) {
	if tx.memo != nil {
		return tx.memo.AppIndex(), nil
	}

	if appID, ok := transaction.AppIDFromTextMemo(tx.textMemo); ok {
		return s.appMapper.GetAppIndex(ctx, appID)
	}

	return 0, nil
}
