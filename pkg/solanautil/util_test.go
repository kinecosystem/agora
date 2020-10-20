package solanautil

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/stretchr/testify/assert"

	commonpb "github.com/kinecosystem/agora-api/genproto/common/v4"
)

func TestCommitmentFromPorot(t *testing.T) {
	cases := []struct {
		input    commonpb.Commitment
		expected solana.Commitment
	}{
		{
			input:    commonpb.Commitment_RECENT,
			expected: solana.CommitmentRecent,
		},
		{
			input:    commonpb.Commitment_SINGLE,
			expected: solana.CommitmentSingle,
		},
		{
			input:    commonpb.Commitment_ROOT,
			expected: solana.CommitmentRoot,
		},
		{
			input:    commonpb.Commitment_MAX,
			expected: solana.CommitmentMax,
		},
		{
			input:    commonpb.Commitment_MAX + 1,
			expected: solana.CommitmentMax,
		},
	}

	for _, c := range cases {
		assert.Equal(t, c.expected, CommitmentFromProto(c.input))
	}
}

func TestTransactionError(t *testing.T) {
	cases := []struct {
		input    string
		expected *commonpb.TransactionError
	}{
		{
			input: fmt.Sprintf(`"%s"`, solana.TransactionErrorBlockhashNotFound),
			expected: &commonpb.TransactionError{
				Reason: commonpb.TransactionError_BAD_NONCE,
			},
		},
		{
			input: fmt.Sprintf(`"%s"`, solana.TransactionErrorAccountNotFound),
			expected: &commonpb.TransactionError{
				Reason: commonpb.TransactionError_INVALID_ACCOUNT,
			},
		},
		{
			input: fmt.Sprintf(`"%s"`, solana.TransactionErrorInvalidAccountForFee),
			expected: &commonpb.TransactionError{
				Reason: commonpb.TransactionError_INVALID_ACCOUNT,
			},
		},
		{
			input: fmt.Sprintf(`"%s"`, solana.TransactionErrorMissingSignatureForFee),
			expected: &commonpb.TransactionError{
				Reason: commonpb.TransactionError_UNAUTHORIZED,
			},
		},
		{
			input: fmt.Sprintf(`"%s"`, solana.TransactionErrorSignatureFailure),
			expected: &commonpb.TransactionError{
				Reason: commonpb.TransactionError_UNAUTHORIZED,
			},
		},
		{
			input: fmt.Sprintf(`{"%s":[1,"%s"]}`, solana.TransactionErrorInstructionError, solana.InstructionErrorGenericError),
			expected: &commonpb.TransactionError{
				Reason:           commonpb.TransactionError_UNKNOWN,
				InstructionIndex: 1,
			},
		},
		{
			input: fmt.Sprintf(`{"%s":[1,"%s"]}`, solana.TransactionErrorInstructionError, solana.InstructionErrorInsufficientFunds),
			expected: &commonpb.TransactionError{
				Reason:           commonpb.TransactionError_INSUFFICIENT_FUNDS,
				InstructionIndex: 1,
			},
		},
		{
			input: fmt.Sprintf(`{"%s":[1,"%s"]}`, solana.TransactionErrorInstructionError, solana.InstructionErrorMissingRequiredSignature),
			expected: &commonpb.TransactionError{
				Reason:           commonpb.TransactionError_UNAUTHORIZED,
				InstructionIndex: 1,
			},
		},
		{
			input: fmt.Sprintf(`{"%s":[1,"%s"]}`, solana.TransactionErrorInstructionError, solana.InstructionErrorUninitializedAccount),
			expected: &commonpb.TransactionError{
				Reason:           commonpb.TransactionError_INVALID_ACCOUNT,
				InstructionIndex: 1,
			},
		},
		{
			input: fmt.Sprintf(`{"%s":[1,"%s"]}`, solana.TransactionErrorInstructionError, solana.InstructionErrorInvalidAccountData),
			expected: &commonpb.TransactionError{
				Reason:           commonpb.TransactionError_INVALID_ACCOUNT,
				InstructionIndex: 1,
			},
		},
		{
			input: fmt.Sprintf(`{"%s":[1,{"%s":%d}]}`, solana.TransactionErrorInstructionError, solana.InstructionErrorCustom, token.ErrorOwnerMismatch),
			expected: &commonpb.TransactionError{
				Reason:           commonpb.TransactionError_UNAUTHORIZED,
				InstructionIndex: 1,
			},
		},
		{
			input: fmt.Sprintf(`{"%s":[1,{"%s":%d}]}`, solana.TransactionErrorInstructionError, solana.InstructionErrorCustom, token.ErrorInsufficientFunds),
			expected: &commonpb.TransactionError{
				Reason:           commonpb.TransactionError_INSUFFICIENT_FUNDS,
				InstructionIndex: 1,
			},
		},
		{
			input: fmt.Sprintf(`{"%s":[1,{"%s":%d}]}`, solana.TransactionErrorInstructionError, solana.InstructionErrorCustom, token.ErrorUninitializedState),
			expected: &commonpb.TransactionError{
				Reason:           commonpb.TransactionError_INVALID_ACCOUNT,
				InstructionIndex: 1,
			},
		},
		{
			input: fmt.Sprintf(`{"%s":[1,{"%s":%d}]}`, solana.TransactionErrorInstructionError, solana.InstructionErrorCustom, token.ErrorInvalidState),
			expected: &commonpb.TransactionError{
				Reason:           commonpb.TransactionError_INVALID_ACCOUNT,
				InstructionIndex: 1,
			},
		},
		{
			input: fmt.Sprintf(`{"%s":[1,{"%s":%d}]}`, solana.TransactionErrorInstructionError, solana.InstructionErrorCustom, token.ErrorAccountFrozen),
			expected: &commonpb.TransactionError{
				Reason:           commonpb.TransactionError_INVALID_ACCOUNT,
				InstructionIndex: 1,
			},
		},
	}

	for _, c := range cases {
		c.expected.Raw = []byte(c.input)

		d := json.NewDecoder(bytes.NewBufferString(c.input))
		var raw interface{}
		assert.NoError(t, d.Decode(&raw))
		txErr, err := solana.ParseTransactionError(raw)
		assert.NoError(t, err)

		actual, err := MapTransactionError(*txErr)
		assert.NoError(t, err)
		assert.True(t, proto.Equal(c.expected, actual))
	}

}
