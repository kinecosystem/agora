package server

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/go/clients/horizon"
	hProtocol "github.com/kinecosystem/go/protocols/horizon"
	"github.com/pkg/errors"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	accountpb "github.com/kinecosystem/agora-api/genproto/account/v3"
	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"

	"github.com/kinecosystem/agora/pkg/account/server/test"
)

func TestAccountNotifier_PaymentOperation(t *testing.T) {
	horizonClient := &horizon.MockClient{}
	accountNotifier := NewAccountNotifier(horizonClient).(*AccountNotifier)

	kp1, acc1 := test.GenerateAccountID(t)
	s1 := newEventStream(5)
	accountNotifier.AddStream(kp1.Address(), s1)

	kp2, acc2 := test.GenerateAccountID(t)
	s2 := newEventStream(5)
	accountNotifier.AddStream(kp2.Address(), s2)

	kp3, acc3 := test.GenerateAccountID(t)
	s3 := newEventStream(5)
	accountNotifier.AddStream(kp3.Address(), s3)

	// Payment from 1 -> 2
	e := test.GenerateTransactionEnvelope(acc1, []xdr.Operation{test.GeneratePaymentOperation(nil, acc2)})
	accountNotifier.NewTransaction(e, test.GenerateTransactionMeta(0, []xdr.OperationMeta{
		{
			Changes: []xdr.LedgerEntryChange{
				test.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc1, 2, 9),
				test.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc2, 1, 11),
			},
		},
	}))

	envBytes, err := e.MarshalBinary()
	require.NoError(t, err)

	assertReceived(t, &accountpb.Events{
		Result: 0,
		Events: []*accountpb.Event{
			getExpectedTransactionEvent(envBytes),
			getExpectedAccountUpdateEvent(kp1.Address(), 2, 9),
		},
	}, false, s1)
	assertReceived(t, &accountpb.Events{
		Result: 0,
		Events: []*accountpb.Event{
			getExpectedTransactionEvent(envBytes),
			getExpectedAccountUpdateEvent(kp2.Address(), 1, 11),
		},
	}, false, s2)
	assertNothingReceived(t, s3)

	// Payment from 2 -> 3, with 1 as a "channel" source
	e = test.GenerateTransactionEnvelope(acc1, []xdr.Operation{test.GeneratePaymentOperation(&acc2, acc3)})
	accountNotifier.NewTransaction(e, test.GenerateTransactionMeta(0, []xdr.OperationMeta{
		{
			Changes: []xdr.LedgerEntryChange{
				test.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc1, 2, 9),
				test.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc2, 1, 5),
				test.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc3, 1, 10),
			},
		},
	}))

	envBytes, err = e.MarshalBinary()
	require.NoError(t, err)

	assertReceived(t, &accountpb.Events{
		Result: 0,
		Events: []*accountpb.Event{
			getExpectedTransactionEvent(envBytes),
			getExpectedAccountUpdateEvent(kp1.Address(), 2, 9),
		},
	}, false, s1)
	assertReceived(t, &accountpb.Events{
		Result: 0,
		Events: []*accountpb.Event{
			getExpectedTransactionEvent(envBytes),
			getExpectedAccountUpdateEvent(kp2.Address(), 1, 5),
		},
	}, false, s2)
	assertReceived(t, &accountpb.Events{
		Result: 0,
		Events: []*accountpb.Event{
			getExpectedTransactionEvent(envBytes),
			getExpectedAccountUpdateEvent(kp3.Address(), 1, 10),
		},
	}, false, s3)
}

func TestAccountNotifier_CreateOperation(t *testing.T) {
	horizonClient := &horizon.MockClient{}
	accountNotifier := NewAccountNotifier(horizonClient).(*AccountNotifier)

	kp1, acc1 := test.GenerateAccountID(t)
	s1 := newEventStream(5)
	accountNotifier.AddStream(kp1.Address(), s1)

	kp2, acc2 := test.GenerateAccountID(t)
	s2 := newEventStream(5)
	accountNotifier.AddStream(kp2.Address(), s2)

	// Create from 1 -> 2
	e := test.GenerateTransactionEnvelope(acc1, []xdr.Operation{test.GenerateCreateOperation(nil, acc2)})
	accountNotifier.NewTransaction(e, test.GenerateTransactionMeta(0, []xdr.OperationMeta{
		{
			Changes: []xdr.LedgerEntryChange{
				test.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc1, 2, 9),
				test.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryCreated, acc2, 1, 1),
			},
		},
	}))

	envBytes, err := e.MarshalBinary()
	require.NoError(t, err)

	assertReceived(t, &accountpb.Events{
		Result: 0,
		Events: []*accountpb.Event{
			getExpectedTransactionEvent(envBytes),
			getExpectedAccountUpdateEvent(kp1.Address(), 2, 9),
		},
	}, false, s1)
	assertReceived(t, &accountpb.Events{
		Result: 0,
		Events: []*accountpb.Event{
			getExpectedTransactionEvent(envBytes),
			getExpectedAccountUpdateEvent(kp2.Address(), 1, 1),
		},
	}, false, s2)
}

func TestAccountNotifier_MergeOperation(t *testing.T) {
	horizonClient := &horizon.MockClient{}
	accountNotifier := NewAccountNotifier(horizonClient).(*AccountNotifier)

	kp1, acc1 := test.GenerateAccountID(t)
	s1 := newEventStream(5)
	accountNotifier.AddStream(kp1.Address(), s1)

	kp2, acc2 := test.GenerateAccountID(t)
	s2 := newEventStream(5)
	accountNotifier.AddStream(kp2.Address(), s2)

	// Merge 1 into 2
	e := test.GenerateTransactionEnvelope(acc1, []xdr.Operation{test.GenerateMergeOperation(nil, acc2)})
	accountNotifier.NewTransaction(e, test.GenerateTransactionMeta(0, []xdr.OperationMeta{
		{
			Changes: []xdr.LedgerEntryChange{
				test.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryRemoved, acc1, 2, 9),
				test.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc2, 1, 1),
			},
		},
	}))

	envBytes, err := e.MarshalBinary()
	require.NoError(t, err)

	assertReceived(t, &accountpb.Events{
		Result: 0,
		Events: []*accountpb.Event{
			getExpectedTransactionEvent(envBytes),
		},
	}, true, s1)
	assertReceived(t, &accountpb.Events{
		Result: 0,
		Events: []*accountpb.Event{
			getExpectedTransactionEvent(envBytes),
			getExpectedAccountUpdateEvent(kp2.Address(), 1, 1),
		},
	}, false, s2)
}

func TestAccountNotifier_MissingAccountInfo(t *testing.T) {
	horizonClient := &horizon.MockClient{}
	accountNotifier := NewAccountNotifier(horizonClient).(*AccountNotifier)

	kp1, acc1 := test.GenerateAccountID(t)
	s1 := newEventStream(5)
	accountNotifier.AddStream(kp1.Address(), s1)

	kp2, acc2 := test.GenerateAccountID(t)
	s2 := newEventStream(5)
	accountNotifier.AddStream(kp2.Address(), s2)

	// Payment from 1 -> 2; missing account data for 2
	meta := test.GenerateTransactionMeta(0, []xdr.OperationMeta{
		{
			Changes: []xdr.LedgerEntryChange{
				test.GenerateLEC(xdr.LedgerEntryChangeTypeLedgerEntryUpdated, acc1, 2, 9),
			},
		},
	})

	horizonClient.On("LoadAccount", kp2.Address()).Return(*test.GenerateHorizonAccount(kp2.Address(), "100", "3"), nil).Once()
	e := test.GenerateTransactionEnvelope(acc1, []xdr.Operation{test.GeneratePaymentOperation(nil, acc2)})
	accountNotifier.NewTransaction(e, meta)

	envBytes, err := e.MarshalBinary()
	require.NoError(t, err)

	assertReceived(t, &accountpb.Events{
		Result: 0,
		Events: []*accountpb.Event{
			getExpectedTransactionEvent(envBytes),
			getExpectedAccountUpdateEvent(kp1.Address(), 2, 9),
		},
	}, false, s1)
	assertReceived(t, &accountpb.Events{
		Result: 0,
		Events: []*accountpb.Event{
			getExpectedTransactionEvent(envBytes),
			getExpectedAccountUpdateEvent(kp2.Address(), 3, 100*100000),
		},
	}, false, s2)

	// Payment from 1 -> 2; missing account data for 2 + fail to fetch from horizon
	horizonClient.On("LoadAccount", kp2.Address()).Return(hProtocol.Account{}, errors.New("some error")).Once()
	e = test.GenerateTransactionEnvelope(acc1, []xdr.Operation{test.GeneratePaymentOperation(nil, acc2)})
	accountNotifier.NewTransaction(e, meta)

	assertReceived(t, &accountpb.Events{
		Result: 0,
		Events: []*accountpb.Event{
			getExpectedTransactionEvent(envBytes),
			getExpectedAccountUpdateEvent(kp1.Address(), 2, 9),
		},
	}, false, s1)
	assertReceived(t, &accountpb.Events{
		Result: 0,
		Events: []*accountpb.Event{
			getExpectedTransactionEvent(envBytes),
		},
	}, false, s2)
}

func assertReceived(t *testing.T, events *accountpb.Events, shouldTerminate bool, s *eventStream) {
	select {
	case n, ok := <-s.streamCh:
		assert.True(t, ok)
		assert.True(t, proto.Equal(events, &n.events))
		assert.Equal(t, shouldTerminate, n.terminateStream)
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

func getExpectedAccountUpdateEvent(addr string, seqNum, balance int64) *accountpb.Event {
	return &accountpb.Event{
		Type: &accountpb.Event_AccountUpdateEvent{
			AccountUpdateEvent: &accountpb.AccountUpdateEvent{
				AccountInfo: &accountpb.AccountInfo{
					AccountId:      &commonpb.StellarAccountId{Value: addr},
					SequenceNumber: seqNum,
					Balance:        balance,
				},
			},
		},
	}
}

func getExpectedTransactionEvent(envelopeXDR []byte) *accountpb.Event {
	return &accountpb.Event{
		Type: &accountpb.Event_TransactionEvent{
			TransactionEvent: &accountpb.TransactionEvent{
				EnvelopeXdr: envelopeXDR,
			},
		},
	}
}
