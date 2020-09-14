package testutil

import (
	"github.com/pkg/errors"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
)

func StellarAccountIDFromString(address string) (id xdr.AccountId, err error) {
	k, err := strkey.Decode(strkey.VersionByteAccountID, address)
	if err != nil {
		return id, errors.New("failed to decode provided address")
	}
	var v xdr.Uint256
	copy(v[:], k)
	return xdr.AccountId{
		Type:    xdr.PublicKeyTypePublicKeyTypeEd25519,
		Ed25519: &v,
	}, nil
}
