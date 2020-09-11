package client

import (
	"context"
	"fmt"
	"runtime"

	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/agora-common/retry"
	"github.com/kinecosystem/go/xdr"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v3"
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"
	"github.com/kinecosystem/agora-api/genproto/transaction/v3"
	transactionpb "github.com/kinecosystem/agora-api/genproto/transaction/v3"
)

const (
	SDKVersion      = "0.2.0"
	userAgentHeader = "kin-user-agent"
)

var (
	userAgent = fmt.Sprintf("KinSDK/%s %s (%s; %s)", SDKVersion, runtime.Version(), runtime.GOOS, runtime.GOARCH)
)

// InternalClient is a low level client used for interacting with
// Agora directly. The API is _not_ stable and is not intend for general use.
//
// It is exposed in case there needs to be low level access to Agora (beyond
// the gRPC client directly). However, there are no stability guarantees between
// releases, or during a migration event.
type InternalClient struct {
	retrier retry.Retrier

	accountClient     accountpb.AccountClient
	transactionClient transactionpb.TransactionClient
}

func NewInternalClient(cc *grpc.ClientConn, retrier retry.Retrier) *InternalClient {
	return &InternalClient{
		retrier:           retrier,
		accountClient:     accountpb.NewAccountClient(cc),
		transactionClient: transactionpb.NewTransactionClient(cc),
	}
}

func (c *InternalClient) GetBlockchainVersion() (int, error) {
	// todo(kin2): support specifying kin2 vs kin3 endpoints
	// todo(kin4): query for migration status of the blockchain.
	//             once the migration to kin4 has happened, we can
	//             aggressively cache this result, and eventually remove
	//             this.
	return 3, nil
}

func (c *InternalClient) CreateStellarAccount(ctx context.Context, key PrivateKey) error {
	ctx = metadata.AppendToOutgoingContext(ctx, userAgentHeader, userAgent)

	_, err := c.retrier.Retry(
		func() error {
			resp, err := c.accountClient.CreateAccount(ctx, &accountpb.CreateAccountRequest{
				AccountId: &commonpb.StellarAccountId{
					Value: key.Public().StellarAddress(),
				},
			})
			if err != nil {
				return err
			}

			switch resp.Result {
			case accountpb.CreateAccountResponse_OK:
				return nil
			case accountpb.CreateAccountResponse_EXISTS:
				return ErrAccountExists
			default:
				return errUnexpectedResult
			}
		},
	)
	return err
}

func (c *InternalClient) GetStellarAccountInfo(ctx context.Context, account PublicKey) (*accountpb.AccountInfo, error) {
	ctx = metadata.AppendToOutgoingContext(ctx, userAgentHeader, userAgent)

	var accountInfo *accountpb.AccountInfo

	_, err := c.retrier.Retry(
		func() error {
			resp, err := c.accountClient.GetAccountInfo(ctx, &accountpb.GetAccountInfoRequest{
				AccountId: &commonpb.StellarAccountId{
					Value: account.StellarAddress(),
				},
			})
			if err != nil {
				return err
			}

			switch resp.Result {
			case accountpb.GetAccountInfoResponse_OK:
				accountInfo = resp.AccountInfo
				return nil
			case accountpb.GetAccountInfoResponse_NOT_FOUND:
				return ErrAccountDoesNotExist
			default:
				return errUnexpectedResult
			}
		},
	)
	if err != nil {
		return nil, err
	}

	return accountInfo, nil
}

func (c *InternalClient) GetTransaction(ctx context.Context, txHash []byte) (data TransactionData, err error) {
	ctx = metadata.AppendToOutgoingContext(ctx, userAgentHeader, userAgent)

	var resp *transactionpb.GetTransactionResponse
	_, err = c.retrier.Retry(func() error {
		resp, err = c.transactionClient.GetTransaction(ctx, &transactionpb.GetTransactionRequest{
			TransactionHash: &commonpb.TransactionHash{
				Value: txHash,
			},
		})
		return err
	})
	if err != nil {
		return TransactionData{}, errors.Wrap(err, "failed to get transaction")
	}

	switch resp.State {
	case transaction.GetTransactionResponse_UNKNOWN:
		return data, ErrTransactionNotFound
	case transactionpb.GetTransactionResponse_SUCCESS:
		data.TxHash = txHash

		var envelope xdr.TransactionEnvelope
		if err := envelope.UnmarshalBinary(resp.Item.EnvelopeXdr); err != nil {
			return data, errors.Wrap(err, "failed to unmarshal xdr")
		}

		var transactionType kin.TransactionType
		memo, ok := kin.MemoFromXDR(envelope.Tx.Memo, true)
		if ok {
			transactionType = memo.TransactionType()
		}

		data.Payments, err = parsePaymentsFromEnvelope(envelope, transactionType, resp.Item.InvoiceList)
		return data, err
	default:
		return TransactionData{}, errors.Errorf("unexpected transaction state from agora: %v", resp.State)
	}
}

type SubmitStellarTransactionResult struct {
	Hash          []byte
	Errors        TransactionErrors
	InvoiceErrors []*transactionpb.SubmitTransactionResponse_InvoiceError
}

func (c *InternalClient) SubmitStellarTransaction(ctx context.Context, envelopeXDR []byte, invoiceList *commonpb.InvoiceList) (result SubmitStellarTransactionResult, err error) {
	ctx = metadata.AppendToOutgoingContext(ctx, userAgentHeader, userAgent)

	var resp *transactionpb.SubmitTransactionResponse
	_, err = c.retrier.Retry(func() error {
		resp, err = c.transactionClient.SubmitTransaction(ctx, &transactionpb.SubmitTransactionRequest{
			EnvelopeXdr: envelopeXDR,
			InvoiceList: invoiceList,
		})
		return err
	})
	if err != nil {
		return result, errors.Wrap(err, "failed to submit transaction")
	}

	result.Hash = resp.Hash.GetValue()

	switch resp.Result {
	case transactionpb.SubmitTransactionResponse_OK:
	case transactionpb.SubmitTransactionResponse_INVOICE_ERROR:
		result.InvoiceErrors = resp.InvoiceErrors
	case transactionpb.SubmitTransactionResponse_FAILED:
		txErrors, err := errorFromXDRBytes(resp.ResultXdr)
		if err != nil {
			return result, errors.Wrap(err, "failed to parse transaction errors")
		}
		result.Errors = txErrors
	default:
		return result, errors.Errorf("unexpected result from agora: %v", resp.Result)
	}

	return result, nil
}
