package cmd

import (
	"testing"
	"time"

	"gotest.tools/assert"
)

func TestRepairMath(t *testing.T) {
	lower := uint64(1_000_000)
	lowerTime := time.Now()
	upper := uint64(2_000_000)
	upperTime := lowerTime.Add(1_000_000 * 400 * time.Millisecond)
	slot := uint64(1_250_000)

	assert.Equal(t, lowerTime.Add(250_000*400*time.Millisecond), getApproximateBlockTime(lower, upper, slot, lowerTime, upperTime))
}
