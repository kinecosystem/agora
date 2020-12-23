package stellar

import (
	"testing"

	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
	"github.com/stretchr/testify/assert"
)

func TestPointer_RoundTrip(t *testing.T) {
	p2 := PointerFromSequence(model.KinVersion_KIN2, 10)
	p3 := PointerFromSequence(model.KinVersion_KIN3, 10)

	for _, p := range []ingestion.Pointer{p2, p3} {
		seq, err := SequenceFromPointer(p)
		assert.NoError(t, err)
		assert.EqualValues(t, seq, 10)
	}
}

func TestPointer_Cursor(t *testing.T) {
	p2 := PointerFromSequence(model.KinVersion_KIN2, 10)
	p3 := PointerFromSequence(model.KinVersion_KIN3, 10)

	for _, p := range []ingestion.Pointer{p2, p3} {
		token, err := CursorFromPointer(p)
		assert.NoError(t, err)
		assert.Equal(t, "42949672960", token)
	}
}
