package client

import (
	"crypto/ed25519"
	"testing"

	"github.com/kinecosystem/go/keypair"
	"github.com/kinecosystem/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeys_Stellar(t *testing.T) {
	kp, err := keypair.Random()
	require.NoError(t, err)

	priv, _ := PrivateKeyFromString(kp.Seed())
	assert.Equal(t, kp.Seed(), priv.stellarSeed())
	assert.Equal(t, kp.Address(), priv.Public().StellarAddress())

	sig, err := kp.Sign([]byte("hello"))
	require.NoError(t, err)
	assert.NoError(t, err)

	// The signatures should match.
	assert.EqualValues(t, ed25519.Sign(ed25519.PrivateKey(priv), []byte("hello")), sig)

	// Verification should work as well.
	pub, err := PublicKeyFromString(kp.Address())
	assert.NoError(t, err)
	assert.True(t, ed25519.Verify(ed25519.PublicKey(pub), []byte("hello"), sig))
	assert.Equal(t, kp.Address(), pub.StellarAddress())

	roundTrip, err := publicKeyFromStellarXDR(accountIDFromPublicKey(pub))
	assert.NoError(t, err)
	assert.EqualValues(t, pub, roundTrip)
}

func TestKeys_StellarErrors(t *testing.T) {
	kp, err := keypair.Random()
	require.NoError(t, err)

	invalidPublicKeys := []string{
		"",
		"abc",
		"Gxx",
		"Sxx",
		kp.Seed(),
	}
	for _, k := range invalidPublicKeys {
		_, err := PublicKeyFromString(k)
		assert.Error(t, err)
	}

	invalidPrivateKeys := []string{
		"",
		"abc",
		"Gxx",
		"Sxx",
		kp.Address(),
	}
	for _, k := range invalidPrivateKeys {
		_, err := PrivateKeyFromString(k)
		assert.Error(t, err)
	}

	invalidAccountXDRs := []xdr.AccountId{
		{
			Type: 1,
		},
	}
	for _, accountID := range invalidAccountXDRs {
		_, err := publicKeyFromStellarXDR(accountID)
		assert.Error(t, err)
	}

}
