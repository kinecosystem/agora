package client

import (
	"context"
	"crypto/sha256"
	"math"
	"time"

	"github.com/golang/protobuf/proto"
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"
	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/agora-common/retry"
	"github.com/kinecosystem/agora-common/retry/backoff"
	"github.com/kinecosystem/agora/pkg/transaction"
	"github.com/kinecosystem/agora/pkg/version"
	"github.com/kinecosystem/go/build"
	"github.com/kinecosystem/go/xdr"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Environment specifies the desired Kin environment to use.
type Environment string

const (
	EnvironmentTest Environment = "test"
	EnvironmentProd Environment = "prod"
)

var (
	testnet = build.Network{Passphrase: "Kin Testnet ; December 2018"}
	mainnet = build.Network{Passphrase: "Kin Mainnet ; December 2018"}

	kin2Testnet  = build.Network{Passphrase: "Kin Playground Network ; June 2018"}
	kin2TMainnet = build.Network{Passphrase: "Public Global Kin Ecosystem Network ; June 2018"}

	kinAssetCode = [4]byte{75, 73, 78, 0} // KIN
)

type Client interface {
	// CreateAccount creates a kin account.
	CreateAccount(ctx context.Context, key PrivateKey) (err error)

	// GetBalance returns the balance of a kin account in quarks.
	//
	// ErrAccountDoesNotExist is returned if no account exists.
	GetBalance(ctx context.Context, account PublicKey) (quarks int64, err error)

	// GetTransaction returns the TransactionData for a given transaction hash.
	//
	// ErrTransactionNotFound is returned if no transaction exists for the hash.
	GetTransaction(ctx context.Context, txHash []byte) (data TransactionData, err error)

	// SubmitPayment submits a single payment to a specified kin account.
	SubmitPayment(ctx context.Context, payment Payment) (txHash []byte, err error)

	// SubmitEarnBatch submits a batch of earn payments.
	//
	// The batch may be done in on or more transactions.
	SubmitEarnBatch(ctx context.Context, batch EarnBatch) (result EarnBatchResult, err error)
}

type client struct {
	internal *InternalClient
	opts     clientOpts

	network build.Network
}

type clientOpts struct {
	maxRetries         uint
	maxSequenceRetries uint
	minDelay           time.Duration
	maxDelay           time.Duration

	cc           *grpc.ClientConn
	endpoint     string
	whitelistKey PrivateKey
	appIndex     uint16

	kinVersion version.KinVersion
	kinIssuer  PublicKey
}

// ClientOption configures a Client.
type ClientOption func(*clientOpts)

// WithKinVersion specifies the version of Kin to use.
//
// If none is provided, the client will default to using the Kin 3 blockchain.
func WithKinVersion(kinVersion version.KinVersion) ClientOption {
	return func(o *clientOpts) {
		o.kinVersion = kinVersion
	}
}

// WithAppIndex specifies the app index to use when
// submitting transactions with Invoices, _or_ to use
// the non-text based memo format.
func WithAppIndex(index uint16) ClientOption {
	return func(o *clientOpts) {
		o.appIndex = index
	}
}

// WithGRPC specifies a grpc.ClientConn to use.
//
// It cannot be used alongside WithEndpoint.
func WithGRPC(cc *grpc.ClientConn) ClientOption {
	return func(o *clientOpts) {
		o.cc = cc
	}
}

// WithEndpoint specifies an endpoint to use.
//
// It cannot be used alongside WithGRPC.
func WithEndpoint(endpoint string) ClientOption {
	return func(o *clientOpts) {
		o.endpoint = endpoint
	}
}

// WithWhitelister specifies a whitelist key that will be used
// to co-sign all transactions.
func WithWhitelister(whitelistKey PrivateKey) ClientOption {
	return func(o *clientOpts) {
		o.whitelistKey = whitelistKey
	}
}

// WithMaxRetries specifies the maximum number of retries the
// client will perform for transient errors.
func WithMaxRetries(maxRetries uint) ClientOption {
	return func(o *clientOpts) {
		o.maxRetries = maxRetries
	}
}

// WithMaxNonceRetries specifies the maximum number of times
// the client will attempt to regenerate a nonce and retry
// a transaction.
//
// This is independent from WithMaxRetries.
func WithMaxNonceRetries(maxSequenceRetries uint) ClientOption {
	return func(o *clientOpts) {
		o.maxSequenceRetries = maxSequenceRetries
	}
}

// WithMinDelay specifies the minimum delay when retrying.
func WithMinDelay(minDelay time.Duration) ClientOption {
	return func(o *clientOpts) {
		o.minDelay = minDelay
	}
}

// WithMaxDelay specifies the maximum delay when retrying.
func WithMaxDelay(maxDelay time.Duration) ClientOption {
	return func(o *clientOpts) {
		o.maxDelay = maxDelay
	}
}

// New creates a new client.
//
// todo: appIndex optional, can use string memo instead
func New(env Environment, opts ...ClientOption) (Client, error) {
	c := &client{
		opts: clientOpts{
			kinVersion:         version.KinVersion3,
			maxRetries:         10,
			maxSequenceRetries: 3,
			minDelay:           500 * time.Millisecond,
			maxDelay:           10 * time.Second,
		},
	}

	for _, o := range opts {
		o(&c.opts)
	}

	var endpoint string
	var issuer string

	switch env {
	case EnvironmentTest:
		endpoint = "api.agorainfra.dev:443"
		if c.opts.kinVersion == 2 {
			c.network = kin2Testnet
			issuer = kin.Kin2TestIssuer
		} else {
			c.network = testnet
		}
	case EnvironmentProd:
		endpoint = "api.agorainfra.net:443"
		if c.opts.kinVersion == 2 {
			c.network = kin2TMainnet
			issuer = kin.Kin2TestIssuer
		} else {
			c.network = mainnet
		}
	default:
		return nil, errors.Errorf("unknown environment: %s", env)
	}

	if c.opts.kinVersion == 2 {
		kinIssuer, err := PublicKeyFromString(issuer)
		if err != nil {
			return nil, errors.New("failed to convert issuer address to public key")
		}

		c.opts.kinIssuer = kinIssuer
	}

	if c.opts.cc != nil && c.opts.endpoint != "" {
		return nil, errors.New("WithGRPC and WithEndpoint cannot both be set")
	}
	if c.opts.endpoint != "" {
		endpoint = c.opts.endpoint
	}

	if c.opts.cc == nil {
		var err error
		c.opts.cc, err = grpc.Dial(endpoint, grpc.WithTransportCredentials(credentials.NewTLS(nil)))
		if err != nil {
			return nil, errors.Wrap(err, "failed to initialize grpc client")
		}
	}

	retrier := retry.NewRetrier(
		retry.Limit(c.opts.maxRetries),
		retry.BackoffWithJitter(backoff.BinaryExponential(c.opts.minDelay), c.opts.maxDelay, 0.1),
		retry.NonRetriableErrors(nonRetriableErrors...),
	)

	c.internal = NewInternalClient(c.opts.cc, retrier, c.opts.kinVersion, version.KinVersionUnknown)

	return c, nil
}

// CreateAccount creates a kin account.
func (c *client) CreateAccount(ctx context.Context, key PrivateKey) error {
	switch c.opts.kinVersion {
	case version.KinVersion2, version.KinVersion3:
		return c.internal.CreateStellarAccount(ctx, key)
	default:
		return errors.Errorf("unsupported kin version: %d", c.opts.kinVersion)
	}
}

// GetBalance returns the balance of a kin account in quarks.
//
// ErrAccountDoesNotExist is returned if no account exists.
func (c *client) GetBalance(ctx context.Context, account PublicKey) (int64, error) {
	switch c.opts.kinVersion {
	case version.KinVersion2, version.KinVersion3:
		accountInfo, err := c.internal.GetStellarAccountInfo(ctx, account)
		if err != nil {
			return 0, err
		}

		return accountInfo.Balance, nil
	default:
		return 0, errors.Errorf("unsupported kin version: %d", c.opts.kinVersion)
	}
}

// GetTransaction returns the TransactionData for a given transaction hash.
//
// ErrTransactionNotFound is returned if no transaction exists for the hash.
func (c *client) GetTransaction(ctx context.Context, txHash []byte) (TransactionData, error) {
	return c.internal.GetStellarTransaction(ctx, txHash)
}

// SubmitPayment sends a single payment to a specified kin account.
func (c *client) SubmitPayment(ctx context.Context, payment Payment) ([]byte, error) {
	if c.opts.kinVersion != 3 && c.opts.kinVersion != 2 {
		return nil, errors.Errorf("unsupported kin version: %d", c.opts.kinVersion)
	}
	if payment.Invoice != nil && c.opts.appIndex == 0 {
		return nil, errors.New("cannot submit payment with invoices without an app index")
	}

	var signers []PrivateKey
	if payment.Channel != nil {
		signers = []PrivateKey{
			*payment.Channel,
			payment.Sender,
		}
	} else {
		signers = []PrivateKey{
			payment.Sender,
		}
	}

	var amount xdr.Int64
	var asset xdr.Asset
	switch c.opts.kinVersion {
	case version.KinVersion2:
		// On Kin 2, the smallest denomination is 1e-7, unlike on Kin 3, where the smallest amount (a quark) is 1e-5.
		// We must therefore convert the provided amount from quarks to the Kin 2 base currency accordingly.
		amount = xdr.Int64(payment.Quarks * 100)
		asset = xdr.Asset{
			Type: xdr.AssetTypeAssetTypeCreditAlphanum4,
			AlphaNum4: &xdr.AssetAlphaNum4{
				Issuer:    accountIDFromPublicKey(c.opts.kinIssuer),
				AssetCode: kinAssetCode,
			},
		}
	default:
		amount = xdr.Int64(payment.Quarks)
		asset = xdr.Asset{
			Type: xdr.AssetTypeAssetTypeNative,
		}
	}

	sender := accountIDFromPublicKey(payment.Sender.Public())
	envelope := xdr.TransactionEnvelope{
		Tx: xdr.Transaction{
			SourceAccount: accountIDFromPublicKey(signers[0].Public()),
			Fee:           100,
			Operations: []xdr.Operation{
				{
					SourceAccount: &sender,
					Body: xdr.OperationBody{
						Type: xdr.OperationTypePayment,
						PaymentOp: &xdr.PaymentOp{
							Destination: accountIDFromPublicKey(payment.Destination),
							Amount:      amount,
							Asset:       asset,
						},
					},
				},
			},
		},
	}

	var invoiceList *commonpb.InvoiceList
	if payment.Memo != "" {
		envelope.Tx.Memo = xdr.Memo{
			Type: xdr.MemoTypeMemoText,
			Text: &payment.Memo,
		}
	} else if c.opts.appIndex > 0 {
		var fk [sha256.Size224]byte

		if payment.Invoice != nil {
			invoiceList = &commonpb.InvoiceList{
				Invoices: []*commonpb.Invoice{
					payment.Invoice,
				},
			}
			invoiceBytes, err := proto.Marshal(invoiceList)
			if err != nil {
				return nil, errors.Wrap(err, "failed to serialize invoice list")
			}
			fk = sha256.Sum224(invoiceBytes)
		}

		memo, err := kin.NewMemo(1, payment.Type, c.opts.appIndex, fk[:])
		if err != nil {
			return nil, errors.Wrap(err, "failed to create memo")
		}

		envelope.Tx.Memo = xdr.Memo{
			Type: xdr.MemoTypeMemoHash,
			Hash: (*xdr.Hash)(&memo),
		}
	}

	result, err := c.signAndSubmitXDR(ctx, signers, envelope, invoiceList)
	if err != nil {
		return result.ID, err
	}

	if len(result.Errors.OpErrors) > 0 {
		if len(result.Errors.OpErrors) != 1 {
			return result.ID, errors.Errorf("invalid number of operation errors. expected 0 or 1, got %d", len(result.Errors.OpErrors))
		}

		return result.ID, result.Errors.OpErrors[0]
	}
	if result.Errors.TxError != nil {
		return result.ID, result.Errors.TxError
	}
	if len(result.InvoiceErrors) > 0 {
		if len(result.InvoiceErrors) != 1 {
			return result.ID, errors.Errorf("invalid number of invoice errors. expected 0 or 1, got %d", len(result.InvoiceErrors))
		}

		switch result.InvoiceErrors[0].Reason {
		case commonpb.InvoiceError_ALREADY_PAID:
			return result.ID, ErrAlreadyPaid
		case commonpb.InvoiceError_WRONG_DESTINATION:
			return result.ID, ErrWrongDestination
		case commonpb.InvoiceError_SKU_NOT_FOUND:
			return result.ID, ErrSKUNotFound
		default:
			return result.ID, errors.Errorf("unknown invoice error: %v", result.InvoiceErrors[0].Reason)
		}
	}

	return result.ID, nil
}

// SubmitEarnBatch submits a batch of earn payments.
//
// The batch may be done in on or more transactions.
func (c *client) SubmitEarnBatch(ctx context.Context, batch EarnBatch) (result EarnBatchResult, err error) {
	// Verify that there isn't a mixed usage of Invoices and text Memos, so we can
	// fail early to reduce the chance of partial failures.
	if batch.Memo != "" {
		for _, r := range batch.Earns {
			if r.Invoice != nil {
				err = errors.New("cannot have invoice set when memo is set")
				break
			}
		}
	} else {
		if batch.Earns[0].Invoice != nil && c.opts.appIndex == 0 {
			err = errors.New("cannot submit earn batch with invoices without an app index")
		} else {
			for i := 0; i < len(batch.Earns)-1; i++ {
				if (batch.Earns[i].Invoice == nil) != (batch.Earns[i+1].Invoice == nil) {
					err = errors.New("either all or none of the earns should have an invoice set")
					break
				}
			}
		}
	}

	if err != nil {
		for _, r := range batch.Earns {
			result.Failed = append(result.Failed, EarnResult{
				Earn: r,
			})
		}
		return result, err
	}

	// Stellar has an operation batch size of 100, so we break apart the EarnBatch into
	// sub-batches of 100 each.
	var batches []EarnBatch
	for start := 0; start < len(batch.Earns); start += 100 {
		end := int(math.Min(float64(start+100), float64(len(batch.Earns))))
		b := EarnBatch{
			Sender:  batch.Sender,
			Channel: batch.Channel,
			Memo:    batch.Memo,
			Earns:   make([]Earn, end-start),
		}
		copy(b.Earns, batch.Earns[start:end])
		batches = append(batches, b)
	}

	lastProcessedBatch := -1
	for i, b := range batches {
		select {
		case <-ctx.Done():
			err = ctx.Err()
		default:
		}
		if err != nil {
			break
		}

		// After this, the batch will always be considered 'processed', as
		// we have performed a submitEarnBatch call, and the results for the batch
		// will be handled (processed).
		lastProcessedBatch = i

		var submitResult SubmitTransactionResult
		submitResult, err = c.submitEarnBatch(ctx, b)
		if err != nil {
			for j := range b.Earns {
				result.Failed = append(result.Failed, EarnResult{
					Earn:  b.Earns[j],
					Error: err,
				})
			}
			break
		}

		// If there are no errors in the result, then the batch was successful.
		if submitResult.Errors.TxError == nil {
			for _, r := range b.Earns {
				result.Succeeded = append(result.Succeeded, EarnResult{
					TxHash: submitResult.ID,
					Earn:   r,
				})
			}

			continue
		}

		// At this point, we consider the batch failed.
		err = submitResult.Errors.TxError

		// If there was operation level errors, we set the individual results
		// for this batch, and then mark the next batch as aborted.
		if len(submitResult.Errors.OpErrors) > 0 {
			for j, e := range submitResult.Errors.OpErrors {
				result.Failed = append(result.Failed, EarnResult{
					TxHash: submitResult.ID,
					Earn:   b.Earns[j],
					Error:  e,
				})
			}
		} else {
			for j := range b.Earns {
				result.Failed = append(result.Failed, EarnResult{
					TxHash: submitResult.ID,
					Earn:   b.Earns[j],
					Error:  err,
				})
			}
		}

		break
	}

	// Add all of the aborted earns to the Failed list.
	for i := lastProcessedBatch + 1; i < len(batches); i++ {
		for _, r := range batches[i].Earns {
			result.Failed = append(result.Failed, EarnResult{
				Earn: r,
			})
		}
	}

	return result, err
}

func (c *client) submitEarnBatch(ctx context.Context, batch EarnBatch) (result SubmitTransactionResult, err error) {
	var signers []PrivateKey
	if batch.Channel != nil {
		signers = []PrivateKey{
			*batch.Channel,
			batch.Sender,
		}
	} else {
		signers = []PrivateKey{
			batch.Sender,
		}
	}

	sender := accountIDFromPublicKey(batch.Sender.Public())
	envelope := xdr.TransactionEnvelope{
		Tx: xdr.Transaction{
			SourceAccount: accountIDFromPublicKey(signers[0].Public()),
			Fee:           100 * xdr.Uint32(len(batch.Earns)),
		},
	}

	var quarksToRaw int64
	var asset xdr.Asset
	switch c.opts.kinVersion {
	case version.KinVersion2:
		// On Kin 2, the smallest denomination is 1e-7, unlike on Kin 3, where the smallest amount (a quark) is 1e-5.
		// We must therefore convert the provided amount from quarks to the Kin 2 base currency accordingly.
		quarksToRaw = 100
		asset = xdr.Asset{
			Type: xdr.AssetTypeAssetTypeCreditAlphanum4,
			AlphaNum4: &xdr.AssetAlphaNum4{
				Issuer:    accountIDFromPublicKey(c.opts.kinIssuer),
				AssetCode: kinAssetCode,
			},
		}
	default:
		quarksToRaw = 1
		asset = xdr.Asset{
			Type: xdr.AssetTypeAssetTypeNative,
		}
	}

	for _, r := range batch.Earns {
		envelope.Tx.Operations = append(envelope.Tx.Operations, xdr.Operation{
			SourceAccount: &sender,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypePayment,
				PaymentOp: &xdr.PaymentOp{
					Destination: accountIDFromPublicKey(r.Destination),
					Amount:      xdr.Int64(r.Quarks * quarksToRaw),
					Asset:       asset,
				},
			},
		})
	}

	var invoiceList *commonpb.InvoiceList
	if batch.Memo != "" {
		envelope.Tx.Memo = xdr.Memo{
			Type: xdr.MemoTypeMemoText,
			Text: &batch.Memo,
		}
	} else {
		invoiceList = &commonpb.InvoiceList{
			Invoices: make([]*commonpb.Invoice, 0, len(batch.Earns)),
		}

		for _, r := range batch.Earns {
			if r.Invoice != nil {
				invoiceList.Invoices = append(invoiceList.Invoices, r.Invoice)
			}
		}

		var fk [sha256.Size224]byte
		if len(invoiceList.Invoices) > 0 {
			if len(invoiceList.Invoices) != len(batch.Earns) {
				return result, errors.Errorf("either all or none of the earns should have an invoice set")
			}
			invoiceBytes, err := proto.Marshal(invoiceList)
			if err != nil {
				return result, errors.Wrap(err, "failed to serialize invoice list")
			}
			fk = sha256.Sum224(invoiceBytes)
		} else {
			invoiceList = nil
		}

		memo, err := kin.NewMemo(1, kin.TransactionTypeEarn, c.opts.appIndex, fk[:])
		if err != nil {
			return result, errors.Wrap(err, "failed to create memo")
		}

		envelope.Tx.Memo = xdr.Memo{
			Type: xdr.MemoTypeMemoHash,
			Hash: (*xdr.Hash)(&memo),
		}
	}

	result, err = c.signAndSubmitXDR(ctx, signers, envelope, invoiceList)
	if err != nil {
		return result, err
	}

	if len(result.InvoiceErrors) > 0 {
		// Invoice errors should not be triggered on earns.
		//
		// This indicates there is something wrong with the service.
		return result, errors.New("unexpected invoice errors present")
	}

	return result, err
}

func (c *client) signAndSubmitXDR(ctx context.Context, signers []PrivateKey, envelope xdr.TransactionEnvelope, invoiceList *commonpb.InvoiceList) (SubmitTransactionResult, error) {
	var result SubmitTransactionResult

	senderInfo, err := c.internal.GetStellarAccountInfo(ctx, signers[0].Public())
	if err != nil {
		return result, err
	}

	offset := int64(1)
	_, err = retry.Retry(
		func() error {
			envelope.Tx.SeqNum = xdr.SequenceNumber(senderInfo.SequenceNumber + offset)

			var signedEnvelope *xdr.TransactionEnvelope
			for _, s := range signers {
				signedEnvelope, err = transaction.SignEnvelope(&envelope, c.network, s.stellarSeed())
				if err != nil {
					return errors.Wrap(err, "failed to sign transaction envelope")
				}
			}

			if len(c.opts.whitelistKey) > 0 {
				signedEnvelope, err = transaction.SignEnvelope(&envelope, c.network, c.opts.whitelistKey.stellarSeed())
				if err != nil {
					return errors.Wrap(err, "failed to sign transaction envelope")
				}
			}

			envelopeBytes, err := signedEnvelope.MarshalBinary()
			if err != nil {
				return errors.Wrap(err, "failed to marshal transaction envelope")
			}

			if result, err = c.internal.SubmitStellarTransaction(ctx, envelopeBytes, invoiceList); err != nil {
				return err
			}
			if result.Errors.TxError == ErrBadNonce {
				offset += 1
				return ErrBadNonce
			}

			return nil
		},
		retry.Limit(c.opts.maxSequenceRetries),
		retry.RetriableErrors(ErrBadNonce),
	)

	return result, err
}
