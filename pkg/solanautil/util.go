package solanautil

import (
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/pkg/errors"

	commonpb "github.com/kinecosystem/agora-api/genproto/common/v4"
)

func CommitmentFromProto(c commonpb.Commitment) solana.Commitment {
	switch c {
	case commonpb.Commitment_RECENT:
		return solana.CommitmentRecent
	case commonpb.Commitment_SINGLE:
		return solana.CommitmentSingle
	case commonpb.Commitment_ROOT:
		return solana.CommitmentRoot
	case commonpb.Commitment_MAX:
		return solana.CommitmentMax
	default:
		return solana.CommitmentMax
	}
}

func MapTransactionError(txError solana.TransactionError) (*commonpb.TransactionError, error) {
	raw, err := txError.JSONString()
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal json")
	}

	result := &commonpb.TransactionError{
		Raw: []byte(raw),
	}

	switch txError.ErrorKey() {
	case solana.TransactionErrorBlockhashNotFound:
		result.Reason = commonpb.TransactionError_BAD_NONCE
	case solana.TransactionErrorAccountNotFound, solana.TransactionErrorInvalidAccountForFee:
		result.Reason = commonpb.TransactionError_INVALID_ACCOUNT
	case solana.TransactionErrorMissingSignatureForFee, solana.TransactionErrorSignatureFailure:
		result.Reason = commonpb.TransactionError_UNAUTHORIZED
	case solana.TransactionErrorInstructionError:
		result.InstructionIndex = int32(txError.InstructionError().Index)

		switch txError.InstructionError().ErrorKey() {
		case solana.InstructionErrorGenericError:
			result.Reason = commonpb.TransactionError_UNKNOWN
		case solana.InstructionErrorInsufficientFunds:
			result.Reason = commonpb.TransactionError_INSUFFICIENT_FUNDS
		case solana.InstructionErrorMissingRequiredSignature:
			result.Reason = commonpb.TransactionError_UNAUTHORIZED
		case solana.InstructionErrorUninitializedAccount, solana.InstructionErrorInvalidAccountData:
			result.Reason = commonpb.TransactionError_INVALID_ACCOUNT
		case solana.InstructionErrorCustom:
			switch *txError.InstructionError().CustomError() {
			case token.ErrorOwnerMismatch:
				result.Reason = commonpb.TransactionError_UNAUTHORIZED
			case token.ErrorInsufficientFunds:
				result.Reason = commonpb.TransactionError_INSUFFICIENT_FUNDS
			case token.ErrorUninitializedState, token.ErrorInvalidState, token.ErrorAccountFrozen:
				result.Reason = commonpb.TransactionError_INVALID_ACCOUNT
			}
		}
	}

	return result, nil
}

func IsAccountAlreadyExistsError(err *solana.TransactionError) bool {
	if err.InstructionError() == nil {
		return false
	}

	ie := err.InstructionError()

	// todo: perhaps we want to formalize the error codes in the system library?
	return ie.Index == 0 && ie.CustomError() != nil && *ie.CustomError() == 0
}
