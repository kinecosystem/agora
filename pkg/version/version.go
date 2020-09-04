package version

import (
	"context"
	"strconv"

	"github.com/kinecosystem/agora-common/headers"
	"github.com/pkg/errors"
)

type KinVersion uint16

const (
	KinVersionUnknown KinVersion = iota
	KinVersionReserved
	KinVersion2
	KinVersion3
)

const (
	kinVersionHeader = "kin-version"
	minVersion       = KinVersion2
	maxVersion       = KinVersion3
	defaultVersion   = KinVersion3
)

// GetCtxKinVersion determines which version of Kin to use based on the headers in the provided context.
func GetCtxKinVersion(ctx context.Context) (version KinVersion, err error) {
	val, err := headers.GetASCIIHeaderByName(ctx, kinVersionHeader)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get kin version header")
	}

	if len(val) == 0 {
		return defaultVersion, nil
	}

	i, err := strconv.Atoi(val)
	if err != nil {
		return 0, errors.Wrap(err, "could not parse integer version from string")
	}

	if i < int(minVersion) || i > int(maxVersion) {
		return 0, errors.Wrap(err, "invalid kin version")
	}

	return KinVersion(i), nil
}
