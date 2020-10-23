package server

import (
	"testing"

	"github.com/kinecosystem/go/clients/horizon"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"

	"github.com/kinecosystem/agora/pkg/testutil"
	"github.com/kinecosystem/agora/pkg/transaction/stellar"
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
	e := testutil.GenerateTransactionEnvelope(acc1, 1, []xdr.Operation{testutil.GeneratePaymentOperation(nil, acc2)})
	r := testutil.GenerateTransactionResult(xdr.TransactionResultCodeTxSuccess, make([]xdr.OperationResult, 0))
	m := testutil.GenerateTransactionMeta(0, []xdr.OperationMeta{
		{
			Changes: []xdr.LedgerEntryChange{
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc1, 2, 9),
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc2, 1, 11),
			},
		},
	})
	xdrData := stellar.XDRData{
		Envelope: e,
		Result:   r,
		Meta:     m,
	}
	accountNotifier.OnTransaction(xdrData)

	assertReceived(t, xdrData, s1)
	assertReceived(t, xdrData, s2)
	assertNothingReceived(t, s3)

	// Payment from 2 -> 3, with 1 as a "channel" source
	e = testutil.GenerateTransactionEnvelope(acc1, 1, []xdr.Operation{testutil.GeneratePaymentOperation(&acc2, acc3)})
	r = testutil.GenerateTransactionResult(xdr.TransactionResultCodeTxFailed, make([]xdr.OperationResult, 0))
	m = testutil.GenerateTransactionMeta(0, []xdr.OperationMeta{
		{
			Changes: []xdr.LedgerEntryChange{
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc1, 2, 9),
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc2, 1, 5),
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc3, 1, 10),
			},
		},
	})
	xdrData = stellar.XDRData{
		Envelope: e,
		Result:   r,
		Meta:     m,
	}
	accountNotifier.OnTransaction(xdrData)

	assertReceived(t, xdrData, s1)
	assertReceived(t, xdrData, s2)
	assertReceived(t, xdrData, s3)
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
	e := testutil.GenerateTransactionEnvelope(acc1, 1, []xdr.Operation{testutil.GenerateCreateOperation(nil, acc2)})
	r := testutil.GenerateTransactionResult(xdr.TransactionResultCodeTxSuccess, make([]xdr.OperationResult, 0))
	m := testutil.GenerateTransactionMeta(0, []xdr.OperationMeta{
		{
			Changes: []xdr.LedgerEntryChange{
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc1, 2, 9),
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryCreated, acc2, 1, 1),
			},
		},
	})
	xdrData := stellar.XDRData{
		Envelope: e,
		Result:   r,
		Meta:     m,
	}
	accountNotifier.OnTransaction(xdrData)

	assertReceived(t, xdrData, s1)
	assertReceived(t, xdrData, s2)
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
	e := testutil.GenerateTransactionEnvelope(acc1, 1, []xdr.Operation{testutil.GenerateMergeOperation(nil, acc2)})
	r := testutil.GenerateTransactionResult(xdr.TransactionResultCodeTxSuccess, make([]xdr.OperationResult, 0))
	m := testutil.GenerateTransactionMeta(0, []xdr.OperationMeta{
		{
			Changes: []xdr.LedgerEntryChange{
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryRemoved, acc1, 2, 9),
				testutil.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc2, 1, 1),
			},
		},
	})
	xdrData := stellar.XDRData{
		Envelope: e,
		Result:   r,
		Meta:     m,
	}
	accountNotifier.OnTransaction(xdrData)

	assertReceived(t, xdrData, s1)
	assertReceived(t, xdrData, s2)
}

func assertReceived(t *testing.T, data stellar.XDRData, s *eventStream) {
	select {
	case actualData, ok := <-s.streamCh:
		assert.True(t, ok)
		assert.Equal(t, data, actualData)
		assert.Equal(t, data, actualData)
		assert.Equal(t, data, actualData)
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
