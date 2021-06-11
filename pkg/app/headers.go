package app

import (
	"context"
	"strconv"

	"github.com/kinecosystem/agora-common/headers"
	"github.com/pkg/errors"
)

const (
	appIndexHeader = "app-index"
)

func GetAppIndex(ctx context.Context) (appIndex uint16, err error) {
	val, err := headers.GetASCIIHeaderByName(ctx, appIndexHeader)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get kin version header")
	}

	if len(val) == 0 {
		return 0, nil
	}

	i, err := strconv.ParseUint(val, 10, 16)
	if err != nil {
		return 0, errors.Wrap(err, "could not parse integer version from string")
	}

	return uint16(i), nil
}
