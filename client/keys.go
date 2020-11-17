package client

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"

	"github.com/kinecosystem/go/xdr"
	"github.com/mr-tron/base58"
	"github.com/pkg/errors"
	"github.com/stellar/go/strkey"
)

// PublicKey is an ed25519.PublicKey.
type PublicKey ed25519.PublicKey

// StellarAddress returns the stellar address representation
// of the public key.
func (k PublicKey) StellarAddress() string {
	return strkey.MustEncode(strkey.VersionByteAccountID, k)
}

// Base58 returns the base58-encoded public key
func (k PublicKey) Base58() string {
	return base58.Encode(k)
}

// PrivateKey is an ed25519.PrivateKey.
type PrivateKey ed25519.PrivateKey

// Public returns the corresponding PublicKey.
func (k PrivateKey) Public() PublicKey {
	return PublicKey(ed25519.PrivateKey(k).Public().(ed25519.PublicKey))
}

// Base58 returns the base58-encoded private key
func (k PrivateKey) Base58() string {
	return base58.Encode(k)
}

func (k PrivateKey) stellarSeed() string {
	return strkey.MustEncode(strkey.VersionByteSeed, k[:32])
}

// PublicKeyFromString parses a provided address, returning
// a PublicKey if successful.
//
// The address may be either a Stellar encoded address, or
// a base58 encoded string.
func PublicKeyFromString(address string) (PublicKey, error) {
	if len(address) == 56 {
		if string(address[0]) == "G" {
			return strkey.Decode(strkey.VersionByteAccountID, address)
		} else if string(address[0]) == "S" {
			return nil, errors.New("address is not a public key")
		}
	}

	raw, err := base58.Decode(address)
	if err != nil {
		return nil, err
	}
	if len(raw) != ed25519.PublicKeySize {
		return nil, errors.Errorf("invalid public key size: %d", len(raw))
	}

	return raw, nil
}

// NewPrivateKey returns a new PrivateKey from derived from
// crypto/rand. The public key can be accessed via key.Public().
func NewPrivateKey() (PrivateKey, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}

	return PrivateKey(priv), nil
}

// PrivateKeyFromString parses a provided address, returning
// a PublicKey if successful.
//
// The address may be either a Stellar encoded seed, or
// a base58 encoded string.
func PrivateKeyFromString(seed string) (PrivateKey, error) {
	if len(seed) != 56 {
		raw, err := base58.Decode(seed)
		if err != nil {
			return nil, err
		}
		if len(raw) != ed25519.PrivateKeySize {
			return nil, errors.Errorf("invalid private key size: %d", len(raw))
		}

		return PrivateKey(raw), nil
	}

	if string(seed[0]) != "S" {
		return nil, errors.New("seed must start with S")
	}

	rawSeed, err := strkey.Decode(strkey.VersionByteSeed, seed)
	if err != nil {
		return nil, err
	}

	_, priv, err := ed25519.GenerateKey(bytes.NewReader(rawSeed))
	return PrivateKey(priv), err
}

func publicKeyFromStellarXDR(id xdr.AccountId) (PublicKey, error) {
	v, ok := id.GetEd25519()
	if !ok {
		return nil, errors.New("xdr.AccountId not an ed25519 key")
	}

	pub := make([]byte, 32)
	copy(pub, v[:])
	return pub, nil
}

func accountIDFromPublicKey(k PublicKey) xdr.AccountId {
	var v xdr.Uint256
	copy(v[:], k)

	return xdr.AccountId{
		Type:    xdr.PublicKeyTypePublicKeyTypeEd25519,
		Ed25519: &v,
	}
}
