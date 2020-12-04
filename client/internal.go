package client

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/agora-common/retry"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/system"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/kinecosystem/go/xdr"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v3"
	accountpbv4 "github.com/kinecosystem/agora-api/genproto/account/v4"
	airdroppbv4 "github.com/kinecosystem/agora-api/genproto/airdrop/v4"
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"
	commonpbv4 "github.com/kinecosystem/agora-api/genproto/common/v4"
	transactionpb "github.com/kinecosystem/agora-api/genproto/transaction/v3"
	transactionpbv4 "github.com/kinecosystem/agora-api/genproto/transaction/v4"

	"github.com/kinecosystem/agora/pkg/version"
)

const (
	SDKVersion              = "0.2.5"
	userAgentHeader         = "kin-user-agent"
	kinVersionHeader        = "kin-version"
	desiredKinVersionHeader = "desired-kin-version"
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
	retrier           retry.Retrier
	kinVersion        version.KinVersion
	desiredKinVersion version.KinVersion

	accountClient     accountpb.AccountClient
	transactionClient transactionpb.TransactionClient

	accountClientV4     accountpbv4.AccountClient
	transactionClientV4 transactionpbv4.TransactionClient
	airdropClientV4     airdroppbv4.AirdropClient

	configMux         sync.Mutex
	serviceConfig     *transactionpbv4.GetServiceConfigResponse
	configLastFetched time.Time
}

func NewInternalClient(cc *grpc.ClientConn, retrier retry.Retrier, kinVersion version.KinVersion, desiredKinVersion version.KinVersion) *InternalClient {
	return &InternalClient{
		retrier:             retrier,
		kinVersion:          kinVersion,
		accountClient:       accountpb.NewAccountClient(cc),
		transactionClient:   transactionpb.NewTransactionClient(cc),
		accountClientV4:     accountpbv4.NewAccountClient(cc),
		transactionClientV4: transactionpbv4.NewTransactionClient(cc),
		airdropClientV4:     airdroppbv4.NewAirdropClient(cc),
		desiredKinVersion:   desiredKinVersion,
	}
}

func (c *InternalClient) GetBlockchainVersion(ctx context.Context) (version.KinVersion, error) {
	ctx = c.addMetadataToCtx(ctx)

	var kinVersion version.KinVersion
	_, err := c.retrier.Retry(
		func() error {
			resp, err := c.transactionClientV4.GetMinimumKinVersion(ctx, &transactionpbv4.GetMinimumKinVersionRequest{})
			if err != nil {
				return err
			}

			kinVersion = version.KinVersion(resp.Version)
			return nil
		},
	)
	if err != nil {
		return version.KinVersionUnknown, err
	}
	return kinVersion, nil
}

func (c *InternalClient) CreateStellarAccount(ctx context.Context, key PrivateKey) error {
	ctx = c.addMetadataToCtx(ctx)

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
	ctx = c.addMetadataToCtx(ctx)

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

func (c *InternalClient) GetStellarTransaction(ctx context.Context, txHash []byte) (data TransactionData, err error) {
	ctx = c.addMetadataToCtx(ctx)

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
	case transactionpb.GetTransactionResponse_UNKNOWN:
		return data, ErrTransactionNotFound
	case transactionpb.GetTransactionResponse_SUCCESS:
		data.TxID = txHash

		var envelope xdr.TransactionEnvelope
		if err := envelope.UnmarshalBinary(resp.Item.EnvelopeXdr); err != nil {
			return data, errors.Wrap(err, "failed to unmarshal xdr")
		}

		var transactionType kin.TransactionType
		memo, ok := kin.MemoFromXDR(envelope.Tx.Memo, true)
		if ok {
			transactionType = memo.TransactionType()
		}

		data.Payments, err = parsePaymentsFromEnvelope(envelope, transactionType, resp.Item.InvoiceList, c.kinVersion)
		return data, err
	default:
		return TransactionData{}, errors.Errorf("unexpected transaction state from agora: %v", resp.State)
	}
}

type SubmitTransactionResult struct {
	ID            []byte
	Errors        TransactionErrors
	InvoiceErrors []*commonpb.InvoiceError
}

func (c *InternalClient) SubmitStellarTransaction(ctx context.Context, envelopeXDR []byte, invoiceList *commonpb.InvoiceList) (result SubmitTransactionResult, err error) {
	ctx = c.addMetadataToCtx(ctx)

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

	result.ID = resp.Hash.GetValue()

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

func (c *InternalClient) CreateSolanaAccount(ctx context.Context, key PrivateKey, commitment commonpbv4.Commitment, subsidizer PrivateKey) (err error) {
	ctx = c.addMetadataToCtx(ctx)

	config, err := c.GetServiceConfig(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get service config")
	}

	if subsidizer == nil && config.GetSubsidizerAccount().GetValue() == nil {
		return ErrNoSubsidizer
	}

	owner := ed25519.PublicKey(key.Public())
	tokenAcc, tokenAccKey := generateTokenAccount(ed25519.PrivateKey(key))
	tokenProgram := config.TokenProgram.Value

	var subsidizerID ed25519.PublicKey
	if subsidizer != nil {
		subsidizerID = ed25519.PublicKey(subsidizer.Public())
	} else {
		subsidizerID = config.SubsidizerAccount.Value
	}

	recentBlockhash, err := c.GetRecentBlockhash(ctx)
	if err != nil {
		return err
	}
	minBalance, err := c.GetMinimumBalanceForRentException(ctx)
	if err != nil {
		return err
	}

	tx := solana.NewTransaction(subsidizerID,
		system.CreateAccount(subsidizerID, tokenAcc, tokenProgram, minBalance, token.AccountSize),
		token.InitializeAccount(tokenAcc, config.Token.Value, owner),
		token.SetAuthority(tokenAcc, owner, subsidizerID, token.AuthorityTypeCloseAccount),
	)
	tx.SetBlockhash(recentBlockhash)

	var signers []ed25519.PrivateKey
	if subsidizer != nil {
		signers = []ed25519.PrivateKey{ed25519.PrivateKey(subsidizer), ed25519.PrivateKey(key), tokenAccKey}
	} else {
		signers = []ed25519.PrivateKey{ed25519.PrivateKey(key), tokenAccKey}
	}
	err = tx.Sign(signers...)
	if err != nil {
		return errors.Wrap(err, "failed to sign transaction")
	}

	var resp *accountpbv4.CreateAccountResponse
	_, err = c.retrier.Retry(func() error {
		resp, err = c.accountClientV4.CreateAccount(ctx, &accountpbv4.CreateAccountRequest{
			Transaction: &commonpbv4.Transaction{
				Value: tx.Marshal(),
			},
			Commitment: commitment,
		})

		return err
	})

	switch resp.Result {
	case accountpbv4.CreateAccountResponse_OK:
		return nil
	case accountpbv4.CreateAccountResponse_EXISTS:
		return ErrAccountExists
	case accountpbv4.CreateAccountResponse_PAYER_REQUIRED:
		return ErrPayerRequired
	case accountpbv4.CreateAccountResponse_BAD_NONCE:
		return ErrBadNonce
	default:
		return errors.Errorf("unexpected result from agora: %v", resp.Result)
	}
}

func (c *InternalClient) GetSolanaAccountInfo(ctx context.Context, account PublicKey, commitment commonpbv4.Commitment) (accountInfo *accountpbv4.AccountInfo, err error) {
	ctx = c.addMetadataToCtx(ctx)

	_, err = c.retrier.Retry(func() error {
		resp, err := c.accountClientV4.GetAccountInfo(ctx, &accountpbv4.GetAccountInfoRequest{
			AccountId:  &commonpbv4.SolanaAccountId{Value: account},
			Commitment: commitment,
		})
		if err != nil {
			return err
		}

		switch resp.Result {
		case accountpbv4.GetAccountInfoResponse_OK:
			accountInfo = resp.AccountInfo
			return nil
		case accountpbv4.GetAccountInfoResponse_NOT_FOUND:
			return ErrAccountDoesNotExist
		default:
			return errors.Errorf("unexpected result from agora: %v", resp.Result)
		}
	})

	if err != nil {
		return nil, err
	}

	return accountInfo, nil
}

func (c *InternalClient) ResolveTokenAccounts(ctx context.Context, publicKey PublicKey) (accounts []PublicKey, err error) {
	ctx = c.addMetadataToCtx(ctx)

	var resp *accountpbv4.ResolveTokenAccountsResponse

	_, err = c.retrier.Retry(func() error {
		resp, err = c.accountClientV4.ResolveTokenAccounts(ctx, &accountpbv4.ResolveTokenAccountsRequest{
			AccountId: &commonpbv4.SolanaAccountId{Value: publicKey},
		})
		return err
	})

	accounts = make([]PublicKey, len(resp.TokenAccounts))
	for i, tokenAccount := range resp.TokenAccounts {
		accounts[i] = tokenAccount.Value
	}
	return accounts, nil
}

func (c *InternalClient) GetTransaction(ctx context.Context, txID []byte, commitment commonpbv4.Commitment) (data TransactionData, err error) {
	ctx = c.addMetadataToCtx(ctx)

	var resp *transactionpbv4.GetTransactionResponse

	_, err = c.retrier.Retry(func() error {
		resp, err = c.transactionClientV4.GetTransaction(ctx, &transactionpbv4.GetTransactionRequest{
			TransactionId: &commonpbv4.TransactionId{
				Value: txID,
			},
			Commitment: commitment,
		})
		return err
	})
	if err != nil {
		return TransactionData{}, errors.Wrap(err, "failed to get transaction")
	}

	data.TxID = txID
	data.TxState = txStateFromProto(resp.State)
	if resp.Item != nil {
		data.Payments, err = parsePaymentsFromProto(resp.Item)
		if err != nil {
			return TransactionData{}, errors.Wrap(err, "failed to parse payments")
		}
		data.Errors, err = errorFromProto(resp.Item.TransactionError)
		if err != nil {
			return TransactionData{}, errors.Wrap(err, "failed to parse error")
		}
	}

	return data, nil
}

func (c *InternalClient) SubmitSolanaTransaction(ctx context.Context, tx solana.Transaction, il *commonpb.InvoiceList, commitment commonpbv4.Commitment) (result SubmitTransactionResult, err error) {
	ctx = c.addMetadataToCtx(ctx)

	attempt := 0

	var resp *transactionpbv4.SubmitTransactionResponse

	_, err = c.retrier.Retry(func() error {
		attempt += 1

		resp, err = c.transactionClientV4.SubmitTransaction(ctx, &transactionpbv4.SubmitTransactionRequest{
			Transaction: &commonpbv4.Transaction{Value: tx.Marshal()},
			InvoiceList: il,
			Commitment:  commitment,
		})
		if err != nil {
			return errors.Wrap(err, "failed to submit transaction")
		}

		if resp.Result == transactionpbv4.SubmitTransactionResponse_ALREADY_SUBMITTED && attempt == 1 {
			return ErrAlreadySubmitted
		}

		return nil
	})
	if err != nil {
		return result, err
	}

	result.ID = resp.Signature.GetValue()

	switch resp.Result {
	case transactionpbv4.SubmitTransactionResponse_OK:
	case transactionpbv4.SubmitTransactionResponse_ALREADY_SUBMITTED:
	case transactionpbv4.SubmitTransactionResponse_REJECTED:
		return result, ErrTransactionRejected
	case transactionpbv4.SubmitTransactionResponse_PAYER_REQUIRED:
		return result, ErrPayerRequired
	case transactionpbv4.SubmitTransactionResponse_FAILED:
		txErrors, err := errorFromProto(resp.TransactionError)
		if err != nil {
			return result, errors.Wrap(err, "failed to parse transaction errors")
		}
		result.Errors = txErrors
	case transactionpbv4.SubmitTransactionResponse_INVOICE_ERROR:
		result.InvoiceErrors = resp.InvoiceErrors
	default:
		return result, errors.Errorf("unexpected result from agora: %v", resp.Result)
	}

	return result, nil
}

func (c *InternalClient) GetServiceConfig(ctx context.Context) (resp *transactionpbv4.GetServiceConfigResponse, err error) {
	ctx = c.addMetadataToCtx(ctx)

	c.configMux.Lock()
	resp = c.serviceConfig
	lastFetched := c.configLastFetched
	c.configMux.Unlock()

	if resp != nil && time.Since(lastFetched) < time.Hour*24 {
		return resp, nil
	}

	_, err = c.retrier.Retry(func() error {
		resp, err = c.transactionClientV4.GetServiceConfig(ctx, &transactionpbv4.GetServiceConfigRequest{})
		return err
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to get service config")
	}

	c.configMux.Lock()
	c.serviceConfig = resp
	c.configLastFetched = time.Now()
	c.configMux.Unlock()

	return resp, nil
}

func (c *InternalClient) GetRecentBlockhash(ctx context.Context) (blockhash solana.Blockhash, err error) {
	ctx = c.addMetadataToCtx(ctx)

	var resp *transactionpbv4.GetRecentBlockhashResponse

	_, err = c.retrier.Retry(func() error {
		resp, err = c.transactionClientV4.GetRecentBlockhash(ctx, &transactionpbv4.GetRecentBlockhashRequest{})
		return err
	})
	if err != nil {
		return blockhash, errors.Wrap(err, "failed to get recent blockhash")
	}

	copy(blockhash[:], resp.Blockhash.Value)
	return blockhash, nil
}

func (c *InternalClient) GetMinimumBalanceForRentException(ctx context.Context) (balance uint64, err error) {
	ctx = c.addMetadataToCtx(ctx)

	var resp *transactionpbv4.GetMinimumBalanceForRentExemptionResponse

	_, err = c.retrier.Retry(func() error {
		resp, err = c.transactionClientV4.GetMinimumBalanceForRentExemption(ctx, &transactionpbv4.GetMinimumBalanceForRentExemptionRequest{})
		return err
	})
	if err != nil {
		return balance, errors.Wrap(err, "failed to get minimum balance for rent exception")
	}

	return resp.Lamports, nil
}

func (c *InternalClient) RequestAirdrop(ctx context.Context, publicKey PublicKey, quarks uint64, commitment commonpbv4.Commitment) (txID []byte, err error) {
	ctx = c.addMetadataToCtx(ctx)

	var resp *airdroppbv4.RequestAirdropResponse

	_, err = c.retrier.Retry(func() error {
		resp, err = c.airdropClientV4.RequestAirdrop(ctx, &airdroppbv4.RequestAirdropRequest{
			AccountId:  &commonpbv4.SolanaAccountId{Value: publicKey},
			Quarks:     quarks,
			Commitment: commitment,
		})
		return err
	})

	switch resp.Result {
	case airdroppbv4.RequestAirdropResponse_OK:
		return resp.Signature.Value, nil
	case airdroppbv4.RequestAirdropResponse_NOT_FOUND:
		return nil, ErrAccountDoesNotExist
	case airdroppbv4.RequestAirdropResponse_INSUFFICIENT_KIN:
		return nil, ErrInsufficientBalance
	default:
		return nil, errors.Errorf("unexpected result from agora: %v", resp.Result)
	}
}

func (c *InternalClient) addMetadataToCtx(ctx context.Context) context.Context {
	ctx = metadata.AppendToOutgoingContext(ctx, userAgentHeader, userAgent, kinVersionHeader, strconv.Itoa(int(c.kinVersion)))
	if c.desiredKinVersion != version.KinVersionUnknown {
		ctx = metadata.AppendToOutgoingContext(ctx, desiredKinVersionHeader, strconv.Itoa(int(c.desiredKinVersion)))
	}
	return ctx
}
