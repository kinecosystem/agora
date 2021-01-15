package solana

import (
	"encoding/binary"
	"math"

	"github.com/pkg/errors"

	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
)

func PointerFromSlot(slot uint64) ingestion.Pointer {
	ptr := make([]byte, 1+8+8)
	ptr[0] = byte(4)
	binary.BigEndian.PutUint64(ptr[1:], slot)
	binary.BigEndian.PutUint64(ptr[1+8:], math.MaxUint64)
	return ptr
}

func SlotFromPointer(p ingestion.Pointer) (slot uint64, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	if len(p) < 9 {
		return 0, errors.Errorf("invalid pointer: %v", p)
	}

	return binary.BigEndian.Uint64(p[1:]), nil
}
