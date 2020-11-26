package client

import (
	"crypto/ed25519"
	"crypto/sha256"
)

func generateTokenAccount(ownerKey ed25519.PrivateKey) (ed25519.PublicKey, ed25519.PrivateKey) {
	tokenAccSeed := sha256.Sum256(ownerKey)
	tokenAccKey := ed25519.NewKeyFromSeed(tokenAccSeed[:])
	return tokenAccKey.Public().(ed25519.PublicKey), tokenAccKey
}
