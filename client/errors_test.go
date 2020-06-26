package client

import (
	"testing"

	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrors_BadParseTransaction(t *testing.T) {
	codes := []xdr.TransactionResultCode{
		xdr.TransactionResultCodeTxTooEarly,
		xdr.TransactionResultCodeTxTooLate,
		xdr.TransactionResultCodeTxInternalError,
	}

	for _, c := range codes {
		result := xdr.TransactionResult{
			Result: xdr.TransactionResultResult{
				Code: c,
			},
		}
		resultBytes, err := result.MarshalBinary()
		require.NoError(t, err)
		_, err = errorFromXDRBytes(resultBytes)
		assert.Error(t, err)
	}
}
func TestErrors_BadParseOperations(t *testing.T) {
	var opCases []xdr.OperationResult
	opCases = append(opCases, xdr.OperationResult{
		Code: xdr.OperationResultCodeOpNotSupported,
	})
	opCases = append(opCases, xdr.OperationResult{
		Code: xdr.OperationResultCodeOpInner,
		Tr: &xdr.OperationResultTr{
			Type: xdr.OperationTypeCreateAccount,
			CreateAccountResult: &xdr.CreateAccountResult{
				Code: xdr.CreateAccountResultCodeCreateAccountLowReserve,
			},
		},
	})
	opCases = append(opCases, xdr.OperationResult{
		Code: xdr.OperationResultCodeOpInner,
		Tr: &xdr.OperationResultTr{
			Type: xdr.OperationTypePayment,
			PaymentResult: &xdr.PaymentResult{
				Code: xdr.PaymentResultCodePaymentLineFull,
			},
		},
	})
	opCases = append(opCases, xdr.OperationResult{
		Code: xdr.OperationResultCodeOpInner,
		Tr: &xdr.OperationResultTr{
			Type: xdr.OperationTypeAccountMerge,
			AccountMergeResult: &xdr.AccountMergeResult{
				Code: xdr.AccountMergeResultCodeAccountMergeMalformed,
			},
		},
	})

	// Test single cases
	for _, c := range opCases {
		results := []xdr.OperationResult{c}
		result := xdr.TransactionResult{
			Result: xdr.TransactionResultResult{
				Code:    xdr.TransactionResultCodeTxFailed,
				Results: &results,
			},
		}

		resultBytes, err := result.MarshalBinary()
		require.NoError(t, err)

		_, err = errorFromXDRBytes(resultBytes)
		assert.Error(t, err, c.Tr)
	}

	// Test combined case
	result := xdr.TransactionResult{
		Result: xdr.TransactionResultResult{
			Code:    xdr.TransactionResultCodeTxFailed,
			Results: &opCases,
		},
	}
	resultBytes, err := result.MarshalBinary()
	require.NoError(t, err)
	_, err = errorFromXDRBytes(resultBytes)
	assert.Error(t, err)
}

func TestErrors_TxError(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code xdr.TransactionResultCode
	}{
		{
			ErrMalformed,
			xdr.TransactionResultCodeTxMissingOperation,
		},
		{
			ErrBadNonce,
			xdr.TransactionResultCodeTxBadSeq,
		},
		{
			ErrInvalidSignature,
			xdr.TransactionResultCodeTxBadAuth,
		},
		{
			ErrInsufficientBalance,
			xdr.TransactionResultCodeTxInsufficientBalance,
		},
		{
			ErrSenderDoesNotExist,
			xdr.TransactionResultCodeTxNoAccount,
		},
		{
			ErrInsufficientFee,
			xdr.TransactionResultCodeTxInsufficientFee,
		},
	} {
		result := xdr.TransactionResult{
			Result: xdr.TransactionResultResult{
				Code: tc.code,
			},
		}

		resultBytes, err := result.MarshalBinary()
		require.NoError(t, err)

		errors, err := errorFromXDRBytes(resultBytes)
		assert.NoError(t, err)
		assert.Equal(t, tc.err, errors.TxError)
		assert.Empty(t, errors.OpErrors)
	}
}

func TestErrors_OpErrors(t *testing.T) {
	type testCase struct {
		expected error
		result   xdr.OperationResult
	}
	var cases []testCase

	// Top level operation codes
	for _, c := range []struct {
		err  error
		code xdr.OperationResultCode
	}{
		{
			ErrInvalidSignature,
			xdr.OperationResultCodeOpBadAuth,
		},
		{
			ErrSenderDoesNotExist,
			xdr.OperationResultCodeOpNoAccount,
		},
	} {
		cases = append(cases, testCase{
			c.err,
			xdr.OperationResult{
				Code: c.code,
			},
		})
	}

	// CreateAccount errors
	for _, c := range []struct {
		err  error
		code xdr.CreateAccountResultCode
	}{
		{
			ErrMalformed,
			xdr.CreateAccountResultCodeCreateAccountMalformed,
		},
		{
			ErrInsufficientBalance,
			xdr.CreateAccountResultCodeCreateAccountUnderfunded,
		},
		{
			ErrAccountExists,
			xdr.CreateAccountResultCodeCreateAccountAlreadyExist,
		},
	} {
		cases = append(cases, testCase{
			c.err,
			xdr.OperationResult{
				Code: xdr.OperationResultCodeOpInner,
				Tr: &xdr.OperationResultTr{
					Type: xdr.OperationTypeCreateAccount,
					CreateAccountResult: &xdr.CreateAccountResult{
						Code: c.code,
					},
				},
			},
		})
	}

	// Payment errors
	for _, c := range []struct {
		err  error
		code xdr.PaymentResultCode
	}{
		{
			ErrMalformed,
			xdr.PaymentResultCodePaymentMalformed,
		},
		{
			ErrInsufficientBalance,
			xdr.PaymentResultCodePaymentUnderfunded,
		},
		{
			ErrMalformed,
			xdr.PaymentResultCodePaymentSrcNoTrust,
		},
		{
			ErrInvalidSignature,
			xdr.PaymentResultCodePaymentSrcNotAuthorized,
		},
		{
			ErrDestinationDoesNotExist,
			xdr.PaymentResultCodePaymentNoDestination,
		},
		{
			ErrMalformed,
			xdr.PaymentResultCodePaymentNoTrust,
		},
		{
			ErrInvalidSignature,
			xdr.PaymentResultCodePaymentNotAuthorized,
		},
	} {
		cases = append(cases, testCase{
			c.err,
			xdr.OperationResult{
				Code: xdr.OperationResultCodeOpInner,
				Tr: &xdr.OperationResultTr{
					Type: xdr.OperationTypePayment,
					PaymentResult: &xdr.PaymentResult{
						Code: c.code,
					},
				},
			},
		})
	}

	cases = append(cases, testCase{
		nil,
		xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypePayment,
				PaymentResult: &xdr.PaymentResult{
					Code: xdr.PaymentResultCodePaymentSuccess,
				},
			},
		},
	})

	var opResults []xdr.OperationResult
	for _, tc := range cases {
		opResults = append(opResults, tc.result)
	}

	result := xdr.TransactionResult{
		Result: xdr.TransactionResultResult{
			Code:    xdr.TransactionResultCodeTxFailed,
			Results: &opResults,
		},
	}

	resultBytes, err := result.MarshalBinary()
	require.NoError(t, err)

	errors, err := errorFromXDRBytes(resultBytes)
	assert.NoError(t, err)
	assert.NotNil(t, errors.TxError)

	require.Len(t, errors.OpErrors, len(cases))

}
