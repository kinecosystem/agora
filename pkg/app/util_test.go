package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsValidAppID(t *testing.T) {
	assert.True(t, IsValidAppID("test"))

	// too short
	assert.False(t, IsValidAppID("te"))

	// too long
	assert.False(t, IsValidAppID("testtest"))

	// invalid characters
	assert.False(t, IsValidAppID("tes!"))
}
