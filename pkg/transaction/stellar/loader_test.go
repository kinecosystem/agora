package stellar

import (
	"context"
	"testing"

	"github.com/kinecosystem/agora-common/headers"
	"github.com/kinecosystem/agora-common/kin/version"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	transactionpb "github.com/kinecosystem/agora-api/genproto/transaction/v3"

	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

func TestStartFromCursor(t *testing.T) {
	ctx, err := headers.ContextWithHeaders(context.Background())
	require.NoError(t, err)

	valid := []struct {
		input    []byte
		expected []byte
	}{
		{input: []byte(""), expected: nil},
		{input: []byte("10"), expected: []byte{byte(model.KinVersion_KIN3), 0, 0, 0, 0, 0, 0, 0, 10}},
		{input: []byte{byte(model.KinVersion_KIN3), 10}, expected: []byte{byte(model.KinVersion_KIN3), 10}},
	}
	for i, tc := range valid {
		actual, err := startFromCursor(ctx, &transactionpb.Cursor{
			Value: tc.input,
		})
		assert.NoError(t, err)
		assert.EqualValues(t, tc.expected, actual, i)
	}

	invalid := [][]byte{
		[]byte("abc"),
		[]byte("1000000000000000000000000"),
	}
	for _, val := range invalid {
		actual, err := startFromCursor(ctx, &transactionpb.Cursor{
			Value: val,
		})
		assert.Error(t, err)
		assert.Nil(t, actual)
	}
}

func TestStartFromCursor_HeaderInference(t *testing.T) {
	cases := []struct {
		headerValue string
		expected    model.KinVersion
	}{
		{
			headerValue: "",
			expected:    model.KinVersion_KIN3,
		},
		{
			headerValue: "2",
			expected:    model.KinVersion_KIN2,
		},
		{
			headerValue: "3",
			expected:    model.KinVersion_KIN3,
		},
		{
			headerValue: "4",
			expected:    model.KinVersion_KIN2,
		},
	}

	for _, tc := range cases {
		ctx, err := headers.ContextWithHeaders(context.Background())
		require.NoError(t, err)
		require.NoError(t, headers.SetASCIIHeader(ctx, version.KinVersionHeader, tc.headerValue))

		actual, err := startFromCursor(ctx, &transactionpb.Cursor{
			Value: []byte("10"),
		})
		assert.NoError(t, err)
		assert.EqualValues(t, []byte{byte(tc.expected), 0, 0, 0, 0, 0, 0, 0, 10}, actual)
	}
}
