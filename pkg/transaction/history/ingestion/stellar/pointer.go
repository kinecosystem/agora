package stellar

import (
	"encoding/binary"
	"strconv"

	"github.com/pkg/errors"

	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

func pointerFromSequence(v model.KinVersion, seq uint32) ingestion.Pointer {
	return model.OrderKeyFromSequence(v, seq)
}

func sequenceFromPointer(p ingestion.Pointer) (seq uint32, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	if len(p) != 5 {
		return 0, errors.Errorf("invalid pointer")
	}

	return binary.BigEndian.Uint32(p[1:]), nil
}

func cursorFromPointer(p ingestion.Pointer) (token string, err error) {
	seq, err := sequenceFromPointer(p)
	if err != nil {
		return "", err
	}

	// we use int64 here as that's what the toid package uses.
	//
	// https://github.com/stellar/go/blob/master/services/horizon/internal/toid/main.go#L156
	return strconv.FormatInt(int64(seq)<<32, 10), nil
}
