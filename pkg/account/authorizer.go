package account

import (
	"bytes"
	"context"
	"crypto/ed25519"

	"github.com/kinecosystem/agora-common/kin"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/memo"
	"github.com/kinecosystem/agora-common/solana/system"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/kinecosystem/agora-common/webhook/createaccount"
	"github.com/mr-tron/base58"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kinecosystem/agora/pkg/app"
	"github.com/kinecosystem/agora/pkg/webhook"
)

type AuthorizationResult int

const (
	AuthorizationResultOK AuthorizationResult = iota
	AuthorizationResultPayerRequired
)

type Authorization struct {
	Result         AuthorizationResult
	Signature      []byte
	Address        ed25519.PublicKey
	Owner          ed25519.PublicKey
	CloseAuthority ed25519.PublicKey
}

type Authorizer interface {
	Authorize(context.Context, solana.Transaction) (Authorization, error)
}

type authorizer struct {
	log                *logrus.Entry
	mapper             app.Mapper
	config             app.ConfigStore
	mint               ed25519.PublicKey
	subsidizer         ed25519.PublicKey
	subsidizerKey      ed25519.PrivateKey
	minAccountLamports uint64

	webhookClient *webhook.Client
	limiter       *Limiter
}

func NewAuthorizer(
	mapper app.Mapper,
	config app.ConfigStore,
	webhookClient *webhook.Client,
	limiter *Limiter,
	minAccountLamports uint64,
	subsidizerKey ed25519.PrivateKey,
	mint ed25519.PublicKey,
) Authorizer {
	a := &authorizer{
		log:           logrus.StandardLogger().WithField("type", "account/authorizer"),
		mapper:        mapper,
		config:        config,
		webhookClient: webhookClient,
		limiter:       limiter,

		minAccountLamports: minAccountLamports,
		mint:               mint,
	}

	if len(subsidizerKey) > 0 {
		a.subsidizerKey = subsidizerKey
		a.subsidizer = subsidizerKey.Public().(ed25519.PublicKey)
	}

	return a
}

// Authorize authorizes (and signs if necessary) a create account transaction.
func (a *authorizer) Authorize(ctx context.Context, tx solana.Transaction) (result Authorization, err error) {
	log := a.log.WithField("method", "Authorize")

	//
	// Parse out memo, if available.
	//
	var offset int
	var appIndex uint16
	if len(tx.Message.Instructions) > 1 {
		m, err := memo.DecompileMemo(tx.Message, 0)
		if err == nil {
			offset++

			memo, err := kin.MemoFromBase64String(string(m.Data), false)
			if err == nil {
				appIndex = memo.AppIndex()
			} else {
				if id, ok := kin.AppIDFromTextMemo(string(m.Data)); ok {
					appIndex, err = a.mapper.GetAppIndex(ctx, id)
					if err != nil && err != app.ErrMappingNotFound {
						log.WithError(err).Warn("failed to get app index")
						return result, status.Errorf(codes.Internal, "failed to get app index: %v", err)
					}
				}
			}
		}
	}

	if appIndex == 0 {
		appIndex, _ = app.GetAppIndex(ctx)
	}
	log.Warnf("Authorize: received appIndex %d", appIndex)

	//
	// Parse out create instruction(s).
	//
	var subsidizer ed25519.PublicKey
	if isSPLAssoc(&tx, offset) {
		create, err := token.DecompileCreateAssociatedAccount(tx.Message, offset)
		if err != nil {
			return result, status.Error(codes.InvalidArgument, "invalid SplAssociatedTokenAccount::CreateAssociatedTokenAccount instruction")
		}
		if !bytes.Equal(create.Mint, a.mint) {
			return result, status.Error(codes.InvalidArgument, "invalid mint")
		}

		offset++
		result.Address = create.Address
		result.Owner = create.Owner
		result.CloseAuthority = create.Owner
		subsidizer = create.Subsidizer
	} else {
		create, err := system.DecompileCreateAccount(tx.Message, offset)
		if err != nil {
			return result, status.Error(codes.InvalidArgument, "invalid System::CreateAccount instruction")
		}
		if create.Size != token.AccountSize {
			return result, status.Errorf(codes.InvalidArgument, "invalid account size. expected %d", token.AccountSize)
		}
		if !bytes.Equal(create.Owner, token.ProgramKey) {
			return result, status.Errorf(codes.InvalidArgument, "invalid account owner. expected %s", base58.Encode(token.ProgramKey))
		}
		if create.Lamports != a.minAccountLamports {
			return result, status.Errorf(codes.InvalidArgument, "invalid amount of lamports. expected: %d", a.minAccountLamports)
		}

		offset++
		initialize, err := token.DecompileInitializeAccount(tx.Message, offset)
		if err != nil {
			return result, status.Error(codes.InvalidArgument, "invalid Token::InitializeAccount instruction")
		}
		if !bytes.Equal(initialize.Account, create.Address) {
			return result, status.Error(codes.InvalidArgument, "different accounts in Sys::CreateAccount and Token::InitializeAccount")
		}
		if !bytes.Equal(initialize.Mint, a.mint) {
			return result, status.Error(codes.InvalidArgument, "Token::InitializeAccount has incorrect mint")
		}

		offset++
		result.Address = create.Address
		result.Owner = initialize.Owner
		result.CloseAuthority = initialize.Owner
		subsidizer = create.Funder
	}

	//
	// Parse out SetAuth, if required
	//
	if !hasValidSignature(&tx, subsidizer) {
		if offset >= len(tx.Message.Instructions) {
			return result, status.Error(codes.InvalidArgument, "missing SplToken::SetAuthority instruction")
		}

		setAuth, err := token.DecompileSetAuthority(tx.Message, offset)
		if err != nil {
			return result, status.Error(codes.InvalidArgument, "invalid SplToken::SetAuthority instruction")
		}

		if setAuth.Type != token.AuthorityTypeCloseAccount {
			return result, status.Error(codes.InvalidArgument, "SplToken::SetAuthority must be of type CloseAccount")
		}
		if !bytes.Equal(setAuth.Account, result.Address) {
			return result, status.Error(codes.InvalidArgument, "SplToken::SetAuthority must be for the created account")
		}
		if !bytes.Equal(setAuth.CurrentAuthority, result.Owner) {
			return result, status.Error(codes.InvalidArgument, "invalid SplToken::SetAuthority owner")
		}
		if !bytes.Equal(setAuth.NewAuthority, subsidizer) {
			return result, status.Error(codes.InvalidArgument, "invalid SplToken::SetAuthority subsidizer")
		}

		result.CloseAuthority = setAuth.NewAuthority
	} else if offset != len(tx.Message.Instructions) {
		return result, status.Error(codes.InvalidArgument, "invalid number of instructions")
	}

	var signed bool
	if bytes.Equal(a.subsidizer, tx.Message.Accounts[0]) {
		allowed, err := a.limiter.Allow(int(appIndex))
		if err != nil {
			log.WithError(err).Warn("failed to check rate limit")
		} else if !allowed {
			return result, status.Error(codes.ResourceExhausted, "rate limited")
		}

		if err := tx.Sign(a.subsidizerKey); err != nil {
			log.WithError(err).Warn("failed to subsidize creation")
			return result, status.Error(codes.Internal, "failed to subsidize creation")
		}

		signed = true
		result.Signature = tx.Signature()
	}

	// If we don't have an app index, we can't proceed further, so we just exit early
	if appIndex == 0 {
		log.Warn("Authorize: did not receive an appIndex, exit early")
		if signed {
			return result, nil
		}

		result.Result = AuthorizationResultPayerRequired
		return result, nil
	}

	cfg, err := a.config.Get(ctx, appIndex)
	if err == app.ErrNotFound || cfg == nil || cfg.CreateAccountURL == nil {
		if err == app.ErrNotFound {
			log.WithError(err).WithField("appIndex", appIndex).Warn("failed to find app config")
			return result, err
		}

		if signed {
			return result, nil
		}

		result.Result = AuthorizationResultPayerRequired
		return result, nil
	} else if err != nil {
		log.WithError(err).WithField("appIndex", appIndex).Warn("failed to get app config")
		return result, status.Error(codes.Internal, "failed to get app config")
	}

	req := &createaccount.Request{
		KinVersion:        4,
		SolanaTransaction: tx.Marshal(),
	}
	createResponse, err := a.webhookClient.CreateAccount(ctx, *cfg.CreateAccountURL, cfg.WebhookSecret, req)
	if err != nil {
		createAccountErr, ok := err.(*webhook.CreateAccountError)
		if !ok {
			log.WithField("appIndex", appIndex).WithError(err).Warn("failed to call create account webhook")
			return result, status.Error(codes.Internal, "failed to verify account creation with webhook")
		}

		log = log.WithField("appIndex", appIndex).WithField("status", createAccountErr.StatusCode)
		switch createAccountErr.StatusCode {
		case 403:
			log.Debug("app server refused account creation")
			result.Result = AuthorizationResultPayerRequired
			return result, nil
		default:
			log.WithError(createAccountErr).Warn("received unexpected error from app server")
			return result, status.Error(codes.Internal, "failed to verify create account with webhook")
		}

	}

	if len(createResponse.Signature) > 0 {
		if len(createResponse.Signature) != ed25519.SignatureSize {
			return result, status.Error(codes.Internal, "webhook returned invalid signature")
		}

		copy(tx.Signatures[0][:], createResponse.Signature)
		result.Signature = createResponse.Signature
	} else if len(result.Signature) == 0 {
		// If there is no signature at this point, it's likely an issue with the webhook.
		//
		// Either we should have signed the transaction, and have simply forwarded the request for approval,
		// or the webhook returned "ok" without a signature, which is an unexpected flow.
		result.Result = AuthorizationResultPayerRequired
		return result, nil
	}

	return result, nil
}

func hasValidSignature(tx *solana.Transaction, account ed25519.PublicKey) bool {
	for i := range tx.Message.Accounts {
		if !bytes.Equal(tx.Message.Accounts[i], account) {
			continue
		}
		if i >= len(tx.Signatures) {
			continue
		}

		// Note: we use ed25519.Verify instead of a simply != solana.Signature{} in case
		// a bad non-zero data has been set.
		if ed25519.Verify(tx.Message.Accounts[i], tx.Message.Marshal(), tx.Signatures[i][:]) {
			return true
		}
	}

	return false
}

func isSPLAssoc(tx *solana.Transaction, index int) bool {
	return bytes.Equal(tx.Message.Accounts[tx.Message.Instructions[index].ProgramIndex], token.AssociatedTokenAccountProgramKey)
}
