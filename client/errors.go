package client

import (
	"github.com/kinecosystem/go/xdr"
	"github.com/pkg/errors"
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
	}
)

// TransactionErrors contains the error details for a transaction.
//
// If TxError is non-nil, the transaction failed.
// OpErrors may or may not be set if TxErrors is set. The length of
// OpErrors will match the number of operations in the transaction.
type TransactionErrors struct {
	TxError  error
	OpErrors []error
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
			return txErrors, errors.Errorf("unknown operation result code: %d", opResult.Code)
		}

		switch opResult.Tr.Type {
		case xdr.OperationTypeCreateAccount:
			switch opResult.Tr.CreateAccountResult.Code {
			case xdr.CreateAccountResultCodeCreateAccountMalformed:
				txErrors.OpErrors[i] = ErrMalformed
			case xdr.CreateAccountResultCodeCreateAccountAlreadyExist:
				txErrors.OpErrors[i] = ErrAccountExists
			case xdr.CreateAccountResultCodeCreateAccountUnderfunded:
				txErrors.OpErrors[i] = ErrInsufficientBalance
			default:
				return txErrors, errors.Errorf("create account operation failed with code: %d", opResult.Tr.CreateAccountResult.Code)
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
				return txErrors, errors.Errorf("payment operation failed with code: %d", opResult.Tr.PaymentResult.Code)
			}
		default:
			return txErrors, errors.Errorf("operation[%d] failed", i)
		}
	}

	return txErrors, nil
}
