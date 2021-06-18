package server

import (
	"crypto/ed25519"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/stellar/go/strkey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v4"

	"github.com/kinecosystem/agora/pkg/events/eventspb"
	"github.com/kinecosystem/agora/pkg/solanautil"
	"github.com/kinecosystem/agora/pkg/testutil"
)

func TestAccountNotifier_OnTransaction(t *testing.T) {
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

	blockTxn = solana.BlockTransaction{
		Transaction: txn,
		Err:         solana.NewTransactionError(solana.TransactionErrorAccountInUse),
	}

	accountNotifier.OnTransaction(blockTxn)

	assertReceived(t, blockTxn, s1)
	assertReceived(t, blockTxn, s2)
	assertNothingReceived(t, s3)
}

func TestAccountNotifier_OnTransactionEvent(t *testing.T) {
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

	accountNotifier.OnEvent(&eventspb.Event{
		SubmissionTime: timestamppb.Now(),
		Kind: &eventspb.Event_TransactionEvent{
			TransactionEvent: &eventspb.TransactionEvent{
				Transaction: txn.Marshal(),
			},
		},
	})

	assertReceived(t, blockTxn, s1)
	assertReceived(t, blockTxn, s2)
	assertNothingReceived(t, s3)

	blockTxn = solana.BlockTransaction{
		Transaction: txn,
		Err:         solana.NewTransactionError(solana.TransactionErrorAccountInUse),
	}

	txErr, err := solanautil.MapTransactionError(*blockTxn.Err)
	require.NoError(t, err)

	rawErr, err := proto.Marshal(txErr)
	require.NoError(t, err)

	accountNotifier.OnEvent(&eventspb.Event{
		SubmissionTime: timestamppb.Now(),
		Kind: &eventspb.Event_TransactionEvent{
			TransactionEvent: &eventspb.TransactionEvent{
				Transaction:      txn.Marshal(),
				TransactionError: rawErr,
			},
		},
	})

	assertReceived(t, blockTxn, s1)
	assertReceived(t, blockTxn, s2)
	assertNothingReceived(t, s3)
}

func TestAccountNotifier_OnSimulationEvent(t *testing.T) {
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

	accountNotifier.OnEvent(&eventspb.Event{
		SubmissionTime: timestamppb.Now(),
		Kind: &eventspb.Event_SimulationEvent{
			SimulationEvent: &eventspb.SimulationEvent{
				Transaction: txn.Marshal(),
			},
		},
	})

	assertReceived(t, blockTxn, s1)
	assertReceived(t, blockTxn, s2)
	assertNothingReceived(t, s3)

	blockTxn = solana.BlockTransaction{
		Transaction: txn,
		Err:         solana.NewTransactionError(solana.TransactionErrorAccountInUse),
	}

	txErr, err := solanautil.MapTransactionError(*blockTxn.Err)
	require.NoError(t, err)

	rawErr, err := proto.Marshal(txErr)
	require.NoError(t, err)

	accountNotifier.OnEvent(&eventspb.Event{
		SubmissionTime: timestamppb.Now(),
		Kind: &eventspb.Event_SimulationEvent{
			SimulationEvent: &eventspb.SimulationEvent{
				Transaction:      txn.Marshal(),
				TransactionError: rawErr,
			},
		},
	})

	assertReceived(t, blockTxn, s1)
	assertReceived(t, blockTxn, s2)
	assertNothingReceived(t, s3)
}

func assertReceived(t *testing.T, txn solana.BlockTransaction, s *eventStream) {
	select {
	case actualData, ok := <-s.streamCh:
		var rawTX []byte
		var rawErr []byte
		switch t := actualData.Type.(type) {
		case *accountpb.Event_SimulationEvent:
			rawTX = t.SimulationEvent.Transaction.Value

			if t.SimulationEvent.TransactionError != nil {
				rawErr = t.SimulationEvent.TransactionError.Raw
			}
		case *accountpb.Event_TransactionEvent:
			rawTX = t.TransactionEvent.Transaction.Value

			if t.TransactionEvent.TransactionError != nil {
				rawErr = t.TransactionEvent.TransactionError.Raw
			}
		}

		assert.True(t, ok)
		assert.Equal(t, txn.Transaction.Marshal(), rawTX)

		if txn.Err != nil {
			jsonString, err := txn.Err.JSONString()
			require.NoError(t, err)
			assert.Equal(t, jsonString, string(rawErr))
		} else {
			assert.Empty(t, rawErr)
		}
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
