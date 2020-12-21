package client

import (
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/kinecosystem/go/xdr"
	"github.com/pkg/errors"

	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"
	commonpbv4 "github.com/kinecosystem/agora-api/genproto/common/v4"
)

var (
	// Query errors.
	ErrAccountExists       = errors.New("account already exists")
	ErrAccountDoesNotExist = errors.New("account does not exist")
	ErrTransactionNotFound = errors.New("transaction not found")

	// Transaction errors.
	ErrMalformed               = errors.New("malformed transaction")
	ErrBadNonce                = errors.New("bad nonce")
	ErrInsufficientBalance     = errors.New("insufficient balance")
	ErrInsufficientFee         = errors.New("insufficient fee")
	ErrSenderDoesNotExist      = errors.New("sender account does not exist")
	ErrDestinationDoesNotExist = errors.New("destination account does not exist")
	ErrInvalidSignature        = errors.New("invalid signature")

	// Invoice Errors
	ErrAlreadyPaid      = errors.New("invoice already paid")
	ErrWrongDestination = errors.New("wrong destination")
	ErrSKUNotFound      = errors.New("sku not found")

	ErrNoSubsidizer        = errors.New("no subsidizer available")
	ErrPayerRequired       = errors.New("payer required")
	ErrTransactionRejected = errors.New("transaction rejected")
	ErrAlreadySubmitted    = errors.New("transaction already submitted")

	errNoTokenAccounts  = errors.New("no token accounts")
	errUnexpectedResult = errors.New("unexpected result from agora")

	// nonRetriableErrors contains the set of errors that
	// should not be retried without modifications to the
	// transaction.
	nonRetriableErrors = []error{
		ErrAccountExists,
		ErrAccountDoesNotExist,
		ErrBadNonce,
		ErrInsufficientBalance,
		ErrTransactionNotFound,
		ErrAlreadyPaid,
		ErrWrongDestination,
		ErrSKUNotFound,
		ErrNoSubsidizer,
		ErrPayerRequired,
		ErrTransactionRejected,
		ErrAlreadySubmitted,
	}
)

// TransactionErrors contains the error details for a transaction.
// If TxError is non-nil, the transaction failed.
type TransactionErrors struct {
	TxError error

	// OpErrors may or may not be set if TxErrors is set. The length of
	// OpErrors will match the number of operations/instructions in the transaction.
	OpErrors []error

	// PaymentErrors may or may not be set if TxErrors is set. If set, the length of
	// PaymentErrors will match the number of payments/transfers in the transaction.
	PaymentErrors []error
}

func errorFromXDRBytes(resultXDR []byte) (txErrors TransactionErrors, err error) {
	var result xdr.TransactionResult
	if err := result.UnmarshalBinary(resultXDR); err != nil {
		return txErrors, errors.Wrap(err, "failed to unmarshal result xdr")
	}

	switch result.Result.Code {
	case xdr.TransactionResultCodeTxSuccess:
		return txErrors, nil
	case xdr.TransactionResultCodeTxMissingOperation:
		txErrors.TxError = ErrMalformed
	case xdr.TransactionResultCodeTxBadSeq:
		txErrors.TxError = ErrBadNonce
	case xdr.TransactionResultCodeTxBadAuth:
		txErrors.TxError = ErrInvalidSignature
	case xdr.TransactionResultCodeTxInsufficientBalance:
		txErrors.TxError = ErrInsufficientBalance
	case xdr.TransactionResultCodeTxNoAccount:
		txErrors.TxError = ErrSenderDoesNotExist
	case xdr.TransactionResultCodeTxInsufficientFee:
		txErrors.TxError = ErrInsufficientFee
	case xdr.TransactionResultCodeTxFailed:
		txErrors.TxError = errors.New("operation failure")
	default:
		return TransactionErrors{}, errors.Errorf("unknown result code: %d", result.Result.Code)
	}

	if result.Result.Code != xdr.TransactionResultCodeTxFailed {
		return txErrors, nil
	}

	txErrors.OpErrors = make([]error, len(*result.Result.Results))

	for i, opResult := range *result.Result.Results {
		switch opResult.Code {
		case xdr.OperationResultCodeOpInner:
		case xdr.OperationResultCodeOpBadAuth:
			txErrors.OpErrors[i] = ErrInvalidSignature
			continue
		case xdr.OperationResultCodeOpNoAccount:
			txErrors.OpErrors[i] = ErrSenderDoesNotExist
			continue
		default:
			txErrors.OpErrors[i] = errors.Errorf("unknown operation result code: %d", opResult.Code)
			continue
		}

		switch opResult.Tr.Type {
		case xdr.OperationTypeCreateAccount:
			switch opResult.Tr.CreateAccountResult.Code {
			case xdr.CreateAccountResultCodeCreateAccountSuccess:
			case xdr.CreateAccountResultCodeCreateAccountMalformed:
				txErrors.OpErrors[i] = ErrMalformed
			case xdr.CreateAccountResultCodeCreateAccountAlreadyExist:
				txErrors.OpErrors[i] = ErrAccountExists
			case xdr.CreateAccountResultCodeCreateAccountUnderfunded:
				txErrors.OpErrors[i] = ErrInsufficientBalance
			default:
				txErrors.OpErrors[i] = errors.Errorf("create account operation failed with code: %d", opResult.Tr.CreateAccountResult.Code)
			}
		case xdr.OperationTypePayment:
			switch opResult.Tr.PaymentResult.Code {
			case xdr.PaymentResultCodePaymentSuccess:
			case xdr.PaymentResultCodePaymentMalformed,
				xdr.PaymentResultCodePaymentNoTrust,
				xdr.PaymentResultCodePaymentSrcNoTrust,
				xdr.PaymentResultCodePaymentNoIssuer:
				txErrors.OpErrors[i] = ErrMalformed
			case xdr.PaymentResultCodePaymentUnderfunded:
				txErrors.OpErrors[i] = ErrInsufficientBalance
			case xdr.PaymentResultCodePaymentSrcNotAuthorized,
				xdr.PaymentResultCodePaymentNotAuthorized:
				txErrors.OpErrors[i] = ErrInvalidSignature
			case xdr.PaymentResultCodePaymentNoDestination:
				txErrors.OpErrors[i] = ErrDestinationDoesNotExist
			default:
				txErrors.OpErrors[i] = errors.Errorf("payment operation failed with code: %d", opResult.Tr.PaymentResult.Code)
			}
		default:
			txErrors.OpErrors[i] = errors.Errorf("operation[%d] failed", i)
		}
	}

	return txErrors, nil
}

func errorsFromSolanaTx(tx *solana.Transaction, protoError *commonpbv4.TransactionError) (txErrors TransactionErrors) {
	e := errorFromProto(protoError)
	if e == nil {
		return txErrors
	}

	txErrors.TxError = e
	if protoError.GetInstructionIndex() >= 0 {
		txErrors.OpErrors = make([]error, len(tx.Message.Instructions))
		txErrors.OpErrors[protoError.GetInstructionIndex()] = e

		paymentErrIndex := protoError.GetInstructionIndex()
		paymentCount := 0

		for i := range tx.Message.Instructions {
			_, err := token.DecompileTransferAccount(tx.Message, i)
			if err == nil {
				paymentCount++
			} else if i < int(protoError.GetInstructionIndex()) {
				paymentErrIndex--
			} else if i == int(protoError.GetInstructionIndex()) {
				paymentErrIndex = -1
			}
		}

		if paymentErrIndex > -1 {
			txErrors.PaymentErrors = make([]error, paymentCount)
			txErrors.PaymentErrors[paymentErrIndex] = e
		}
	}

	return txErrors
}

func errorsFromStellarTx(env xdr.TransactionEnvelope, protoError *commonpbv4.TransactionError) (txErrors TransactionErrors) {
	e := errorFromProto(protoError)
	if e == nil {
		return txErrors
	}

	txErrors.TxError = e
	if protoError.GetInstructionIndex() >= 0 {
		txErrors.OpErrors = make([]error, len(env.Tx.Operations))
		txErrors.OpErrors[protoError.GetInstructionIndex()] = e

		paymentErrIndex := protoError.GetInstructionIndex()
		paymentCount := 0
		for i, op := range env.Tx.Operations {
			if op.Body.Type == xdr.OperationTypePayment {
				paymentCount++
			} else if i < int(protoError.GetInstructionIndex()) {
				paymentErrIndex--
			} else if i == int(protoError.GetInstructionIndex()) {
				paymentErrIndex = -1
			}
		}

		if paymentErrIndex > -1 {
			txErrors.PaymentErrors = make([]error, paymentCount)
			txErrors.PaymentErrors[paymentErrIndex] = e
		}
	}

	return txErrors
}

func errorFromProto(protoError *commonpbv4.TransactionError) error {
	if protoError == nil {
		return nil
	}

	switch protoError.Reason {
	case commonpbv4.TransactionError_NONE:
		return nil
	case commonpbv4.TransactionError_UNKNOWN:
		return errors.New("unknown error")
	case commonpbv4.TransactionError_UNAUTHORIZED:
		return ErrInvalidSignature
	case commonpbv4.TransactionError_BAD_NONCE:
		return ErrBadNonce
	case commonpbv4.TransactionError_INSUFFICIENT_FUNDS:
		return ErrInsufficientBalance
	case commonpbv4.TransactionError_INVALID_ACCOUNT:
		return ErrAccountDoesNotExist
	default:
		return errors.Errorf("unknown error reason: %d", protoError.Reason)
	}
}

func invoiceErrorFromProto(protoError *commonpb.InvoiceError) error {
	if protoError == nil {
		return nil
	}

	switch protoError.Reason {
	case commonpb.InvoiceError_ALREADY_PAID:
		return ErrAlreadyPaid
	case commonpb.InvoiceError_WRONG_DESTINATION:
		return ErrWrongDestination
	case commonpb.InvoiceError_SKU_NOT_FOUND:
		return ErrSKUNotFound
	default:
		return errors.Errorf("unknown invoice error: %v", protoError.Reason)
	}
}
