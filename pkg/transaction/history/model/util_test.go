package model

import (
	"crypto/ed25519"
	"encoding/binary"
	"math/rand"
	"strconv"
	"testing"

	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/kinecosystem/go/strkey"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/testutil"
)

func TestStellar(t *testing.T) {
	accounts := make([]xdr.AccountId, 6)
	for i := 0; i < len(accounts); i++ {
		_, accounts[i] = testutil.GenerateAccountID(t)
	}

	_, src := testutil.GenerateAccountID(t)
	envelope := testutil.GenerateTransactionEnvelope(
		src,
		1,
		[]xdr.Operation{
			testutil.GenerateCreateOperation(&accounts[0], accounts[1]),
			testutil.GeneratePaymentOperation(&accounts[2], accounts[3]),
			testutil.GenerateMergeOperation(&accounts[4], accounts[5]),
		},
	)

	envelopeBytes, err := envelope.MarshalBinary()
	require.NoError(t, err)

	networkPassphrase := "network phassphrase"
	expected, err := network.HashTransaction(&envelope.Tx, networkPassphrase)
	require.NoError(t, err)

	e := Entry{
		Version: KinVersion_KIN3,
		Kind: &Entry_Stellar{
			Stellar: &StellarEntry{
				Ledger:            10,
				PagingToken:       1,
				NetworkPassphrase: networkPassphrase,
				EnvelopeXdr:       envelopeBytes,
			},
		},
	}

	envelopeAccounts, err := GetAccountsFromEnvelope(envelope)
	assert.NoError(t, err)
	assert.Len(t, envelopeAccounts, 1+len(accounts))
	for _, account := range append([]xdr.AccountId{src}, accounts...) {
		_, exists := envelopeAccounts[account.Address()]
		assert.True(t, exists)
	}

	// ID
	actual, err := e.GetTxID()
	assert.NoError(t, err)
	assert.EqualValues(t, expected[:], actual)

	// Accounts
	entryAccounts, err := e.GetAccounts()
	assert.NoError(t, err)
	assert.Equal(t, len(entryAccounts), len(envelopeAccounts))
	for _, account := range entryAccounts {
		_, exists := envelopeAccounts[account]
		assert.True(t, exists)
	}

	// Ordering Key
	for _, v := range []KinVersion{KinVersion_KIN2, KinVersion_KIN3} {
		e.Version = v

		k, err := e.GetOrderingKey()
		assert.NoError(t, err)

		pt := e.Kind.(*Entry_Stellar).Stellar.PagingToken
		cursor := strconv.FormatUint(pt, 10)

		actual, err := OrderingKeyFromCursor(v, cursor)
		assert.NoError(t, err)
		assert.EqualValues(t, actual, k)
	}
}

func TestSolana(t *testing.T) {
	sender := testutil.GenerateSolanaKeypair(t)
	dest := testutil.GenerateSolanaKeypair(t)
	txn := solana.NewTransaction(
		sender.Public().(ed25519.PublicKey),
		token.Transfer(
			sender.Public().(ed25519.PublicKey),
			dest.Public().(ed25519.PublicKey),
			sender.Public().(ed25519.PublicKey),
			10,
		),
	)
	assert.NoError(t, txn.Sign(sender))

	entry := &Entry{
		Version: KinVersion_KIN4,
		Kind: &Entry_Solana{
			Solana: &SolanaEntry{
				Slot:        1,
				Confirmed:   true,
				Transaction: txn.Marshal(),
			},
		},
	}

	// Hash
	txID, err := entry.GetTxID()
	assert.NoError(t, err)
	assert.Equal(t, txn.Signature(), txID)

	// Accounts
	txnAccounts, err := GetAccountsFromTransaction(txn)
	assert.NoError(t, err)
	assert.Len(t, txnAccounts, 2)
	for _, account := range []ed25519.PrivateKey{sender, dest} {
		a := strkey.MustEncode(strkey.VersionByteAccountID, account.Public().(ed25519.PublicKey))
		_, exists := txnAccounts[a]
		assert.True(t, exists)
	}

	accounts, err := entry.GetAccounts()
	assert.NoError(t, err)
	for _, account := range accounts {
		_, exists := txnAccounts[account]
		assert.True(t, exists)
	}

	// Ordering Key
	k, err := entry.GetOrderingKey()
	assert.NoError(t, err)

	assert.EqualValues(t, KinVersion_KIN4, k[0])
	assert.Equal(t, entry.Kind.(*Entry_Solana).Solana.Slot, binary.BigEndian.Uint64(k[1:]))
	assert.Equal(t, txID[:8], k[9:])
}

func TestAccountFromRaw(t *testing.T) {
	var seed [32]byte
	_, err := rand.New(rand.NewSource(0)).Read(seed[:])
	require.NoError(t, err)

	rawKey := ed25519.NewKeyFromSeed(seed[:])
	require.NoError(t, err)
	kp, err := keypair.FromRawSeed(seed)
	require.NoError(t, err)

	actual := accountFromRaw(rawKey.Public().(ed25519.PublicKey))
	assert.Equal(t, kp.Address(), actual)
}

func TestOrderingKeyFromBlock(t *testing.T) {
	v := OrderingKeyFromBlock(0x1122334455667788)
	assert.EqualValues(t, KinVersion_KIN4, v[0])
	assert.Equal(t, uint64(0x1122334455667788), binary.BigEndian.Uint64(v[1:]))
	for i := 0; i < 8; i++ {
		assert.EqualValues(t, 0, v[9+i])
	}

	block, err := BlockFromOrderingKey(v)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0x1122334455667788), block)

	orderingKey, err := OrderingKeyFromCursor(KinVersion_KIN3, "10")
	require.NoError(t, err)
	_, err = BlockFromOrderingKey(orderingKey)
	assert.Equal(t, ErrInvalidOrderingKeyVersion, err)

}
