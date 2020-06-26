package server

import (
	"testing"

	transactionpb "github.com/kinecosystem/agora-api/genproto/transaction/v3"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
	"github.com/stretchr/testify/assert"
)

func TestStartFromCursor(t *testing.T) {
	valid := []struct {
		input    []byte
		expected []byte
	}{
		{input: []byte(""), expected: nil},
		{input: []byte("10"), expected: []byte{byte(model.KinVersion_KIN3), 0, 0, 0, 0, 0, 0, 0, 10}},
		{input: []byte{byte(model.KinVersion_KIN3), 10}, expected: []byte{byte(model.KinVersion_KIN3), 10}},
	}
	for _, tc := range valid {
		actual, err := startFromCursor(&transactionpb.Cursor{
			Value: tc.input,
		})
		assert.NoError(t, err)
		assert.EqualValues(t, tc.expected, actual)
	}

	invalid := [][]byte{
		[]byte("abc"),
		[]byte("1000000000000000000000000"),
	}
	for _, val := range invalid {
		actual, err := startFromCursor(&transactionpb.Cursor{
			Value: val,
		})
		assert.Error(t, err)
		assert.Nil(t, actual)
	}
}
