package solana

import (
	"encoding/binary"

	"github.com/pkg/errors"

	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
)

func pointerFromSlot(slot uint64) ingestion.Pointer {
	ptr := make([]byte, 9)
	ptr[0] = byte(4)
	binary.BigEndian.PutUint64(ptr[1:], slot)
	return ptr
}

func slotFromPointer(p ingestion.Pointer) (slot uint64, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	if len(p) != 9 {
		return 0, errors.Errorf("invalid pointer")
	}

	return binary.BigEndian.Uint64(p[1:]), nil
}
