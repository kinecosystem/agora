package solana

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPointer_RoundTrip(t *testing.T) {
	expected := uint64(1234567890)
	actual, err := SlotFromPointer(PointerFromSlot(expected))
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)

	_, err = SlotFromPointer([]byte{0})
	assert.NotNil(t, err)
}
