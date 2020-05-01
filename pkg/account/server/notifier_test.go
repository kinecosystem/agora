package server

import (
	"testing"

	"github.com/kinecosystem/go/clients/horizon"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"

	"github.com/kinecosystem/agora/pkg/testutil"
)

func TestAccountNotifier_PaymentOperation(t *testing.T) {
	horizonClient := &horizon.MockClient{}
	accountNotifier := NewAccountNotifier(horizonClient)

	kp1, acc1 := testutil.GenerateAccountID(t)
	s1 := newEventStream(5)
	accountNotifier.AddStream(kp1.Address(), s1)

	kp2, acc2 := testutil.GenerateAccountID(t)
	s2 := newEventStream(5)
	accountNotifier.AddStream(kp2.Address(), s2)

	kp3, acc3 := testutil.GenerateAccountID(t)
	s3 := newEventStream(5)
	accountNotifier.AddStream(kp3.Address(), s3)

	// Payment from 1 -> 2
	e := testutil.GenerateTransactionEnvelope(acc1, []xdr.Operation{testutil.GeneratePaymentOperation(nil, acc2)})
	m := testutil.GenerateTransactionMeta(0, []xdr.OperationMeta{
		{
			Changes: []xdr.LedgerEntryChange{
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc1, 2, 9),
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc2, 1, 11),
			},
		},
	})
	accountNotifier.OnTransaction(e, m)

	assertReceived(t, e, m, s1)
	assertReceived(t, e, m, s2)
	assertNothingReceived(t, s3)

	// Payment from 2 -> 3, with 1 as a "channel" source
	e = testutil.GenerateTransactionEnvelope(acc1, []xdr.Operation{testutil.GeneratePaymentOperation(&acc2, acc3)})
	m = testutil.GenerateTransactionMeta(0, []xdr.OperationMeta{
		{
			Changes: []xdr.LedgerEntryChange{
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc1, 2, 9),
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc2, 1, 5),
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc3, 1, 10),
			},
		},
	})
	accountNotifier.OnTransaction(e, m)

	assertReceived(t, e, m, s1)
	assertReceived(t, e, m, s2)
	assertReceived(t, e, m, s3)
}

func TestAccountNotifier_CreateOperation(t *testing.T) {
	horizonClient := &horizon.MockClient{}
	accountNotifier := NewAccountNotifier(horizonClient)

	kp1, acc1 := testutil.GenerateAccountID(t)
	s1 := newEventStream(5)
	accountNotifier.AddStream(kp1.Address(), s1)

	kp2, acc2 := testutil.GenerateAccountID(t)
	s2 := newEventStream(5)
	accountNotifier.AddStream(kp2.Address(), s2)

	// Create from 1 -> 2
	e := testutil.GenerateTransactionEnvelope(acc1, []xdr.Operation{testutil.GenerateCreateOperation(nil, acc2)})
	m := testutil.GenerateTransactionMeta(0, []xdr.OperationMeta{
		{
			Changes: []xdr.LedgerEntryChange{
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc1, 2, 9),
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryCreated, acc2, 1, 1),
			},
		},
	})
	accountNotifier.OnTransaction(e, m)

	assertReceived(t, e, m, s1)
	assertReceived(t, e, m, s2)
}

func TestAccountNotifier_MergeOperation(t *testing.T) {
	horizonClient := &horizon.MockClient{}
	accountNotifier := NewAccountNotifier(horizonClient)

	kp1, acc1 := testutil.GenerateAccountID(t)
	s1 := newEventStream(5)
	accountNotifier.AddStream(kp1.Address(), s1)

	kp2, acc2 := testutil.GenerateAccountID(t)
	s2 := newEventStream(5)
	accountNotifier.AddStream(kp2.Address(), s2)

	// Merge 1 into 2
	e := testutil.GenerateTransactionEnvelope(acc1, []xdr.Operation{testutil.GenerateMergeOperation(nil, acc2)})
	m := testutil.GenerateTransactionMeta(0, []xdr.OperationMeta{
		{
			Changes: []xdr.LedgerEntryChange{
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryRemoved, acc1, 2, 9),
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc2, 1, 1),
			},
		},
	})
	accountNotifier.OnTransaction(e, m)

	assertReceived(t, e, m, s1)
	assertReceived(t, e, m, s2)
}

func assertReceived(t *testing.T, e xdr.TransactionEnvelope, m xdr.TransactionMeta, s *eventStream) {
	select {
	case actualData, ok := <-s.streamCh:
		assert.True(t, ok)
		assert.Equal(t, e, actualData.e)
		assert.Equal(t, m, actualData.m)
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
