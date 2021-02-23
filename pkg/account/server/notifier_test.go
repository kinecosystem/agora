package server

import (
	"crypto/ed25519"
	"testing"

	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/stellar/go/strkey"
	"github.com/stretchr/testify/assert"

	"github.com/kinecosystem/agora/pkg/testutil"
)

func TestAccountNotifier_Transfer(t *testing.T) {
	accountNotifier := NewAccountNotifier()

	kp1, _ := testutil.GenerateAccountID(t)
	s1 := newEventStream(5)
	accountNotifier.AddStream(kp1.Address(), s1)

	kp2, _ := testutil.GenerateAccountID(t)
	s2 := newEventStream(5)
	accountNotifier.AddStream(kp2.Address(), s2)

	kp3, _ := testutil.GenerateAccountID(t)
	s3 := newEventStream(5)
	accountNotifier.AddStream(kp3.Address(), s3)

	// Payment from 1 -> 2
	subsidizer := testutil.GenerateSolanaKeypair(t)
	txn := solana.NewTransaction(
		subsidizer.Public().(ed25519.PublicKey),
		token.Transfer(
			strkey.MustDecode(strkey.VersionByteAccountID, kp1.Address()),
			strkey.MustDecode(strkey.VersionByteAccountID, kp2.Address()),
			strkey.MustDecode(strkey.VersionByteAccountID, kp1.Address()),
			10,
		),
	)
	assert.NoError(t, txn.Sign(subsidizer))
	blockTxn := solana.BlockTransaction{
		Transaction: txn,
	}

	accountNotifier.OnTransaction(solana.BlockTransaction{
		Transaction: txn,
	})

	assertReceived(t, blockTxn, s1)
	assertReceived(t, blockTxn, s2)
	assertNothingReceived(t, s3)
}

func assertReceived(t *testing.T, txn solana.BlockTransaction, s *eventStream) {
	select {
	case actualData, ok := <-s.streamCh:
		assert.True(t, ok)
		assert.Equal(t, txn, actualData)
	default:
		t.Fatalf("should have received a value")
	}
}

func assertNothingReceived(t *testing.T, s *eventStream) {
	select {
	case <-s.streamCh:
		t.Fatalf("should not have received a value")
	default:
	}
}
