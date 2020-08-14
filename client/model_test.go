package client

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKinToQuarks(t *testing.T) {
	validCases := map[string]int64{
		"0.00001": 1,
		"0.00002": 2,
		"1":       1e5,
		"2":       2e5,
		// 10 trillion, more than what's in cicrulation
		"10000000000000": 1e13 * 1e5,
	}
	for in, expected := range validCases {
		actual, err := KinToQuarks(in)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)

		if strings.Contains(in, ".") {
			assert.Equal(t, in, QuarksToKin(expected))
		} else {
			assert.Equal(t, fmt.Sprintf("%s.00000", in), QuarksToKin(expected))
		}
	}
	invalidCases := []string{
		"0.000001",
		"0.000015",
	}
	for _, in := range invalidCases {
		actual, err := KinToQuarks(in)
		assert.Error(t, err)
		assert.Equal(t, int64(0), actual)
	}
}
