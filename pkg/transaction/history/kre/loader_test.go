package kre

import (
	"context"
	"crypto/rand"
	"sync"
	"testing"
	"time"

	"github.com/kinecosystem/agora-common/solana"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	memory "github.com/kinecosystem/agora/pkg/app/memory/mapper"
	"github.com/kinecosystem/agora/pkg/testutil"
	"github.com/kinecosystem/agora/pkg/transaction/history"
)

func TestLoader(t *testing.T) {
	creationSubmitter := newTestSubmitter()
	paymentSubmitter := newTestSubmitter()
	mapper := memory.New()
	require.NoError(t, mapper.Add(context.Background(), "test", 32))

	l := NewLoader(creationSubmitter, paymentSubmitter, mapper)

	subsidizer := testutil.GenerateSolanaKeys(t, 1)[0]
	account := testutil.GenerateSolanaKeys(t, 1)[0]
	accountOwner := testutil.GenerateSolanaKeys(t, 1)[0]
	dest := testutil.GenerateSolanaKeys(t, 1)[0]
	destOwner := testutil.GenerateSolanaKeys(t, 1)[0]
	appID := "1-test"
	appIDNotFound := "1-nope"
	text := "blah"

	signature := func() solana.Signature {
		var s solana.Signature
		_, _ = rand.Read(s[:])
		return s
	}

	creations := []*history.Creation{
		{
			TxID:         signature(),
			BlockTime:    time.Now(),
			Offset:       10,
			Successful:   true,
			Account:      account,
			AccountOwner: accountOwner,
			MemoText:     &appID,
			Memo:         nil,
			AppIndex:     10,
			Subsidizer:   subsidizer,
		},
		{
			TxID:         signature(),
			BlockTime:    time.Now(),
			Offset:       11,
			Successful:   true,
			Account:      account,
			AccountOwner: accountOwner,
			MemoText:     &appID,
			Memo:         nil,
			Subsidizer:   subsidizer,
		},
		{
			TxID:         signature(),
			BlockTime:    time.Now(),
			Offset:       11,
			Successful:   true,
			Account:      account,
			AccountOwner: accountOwner,
			MemoText:     &appIDNotFound,
			Memo:         nil,
			Subsidizer:   subsidizer,
		},
		{
			TxID:         signature(),
			BlockTime:    time.Now(),
			Offset:       11,
			Successful:   true,
			Account:      account,
			AccountOwner: accountOwner,
			MemoText:     &text,
			Memo:         nil,
			Subsidizer:   subsidizer,
		},
	}
	payments := []*history.Payment{
		{
			TxID:        signature(),
			BlockTime:   time.Now(),
			Offset:      10,
			Successful:  true,
			Source:      account,
			SourceOwner: accountOwner,
			Dest:        dest,
			DestOwner:   destOwner,
			Quarks:      1234,
			MemoText:    &appID,
			Memo:        nil,
			AppIndex:    10,
			Subsidizer:  subsidizer,
		},
		{
			TxID:        signature(),
			BlockTime:   time.Now(),
			Offset:      12,
			Successful:  true,
			Source:      account,
			SourceOwner: accountOwner,
			Dest:        dest,
			DestOwner:   destOwner,
			Quarks:      1235,
			MemoText:    &appIDNotFound,
			Memo:        nil,
			Subsidizer:  subsidizer,
		},
		{
			TxID:        signature(),
			BlockTime:   time.Now(),
			Offset:      12,
			Successful:  true,
			Source:      account,
			SourceOwner: accountOwner,
			Dest:        dest,
			DestOwner:   destOwner,
			Quarks:      1235,
			MemoText:    &appID,
			Memo:        nil,
			Subsidizer:  subsidizer,
		},
		{
			TxID:        signature(),
			BlockTime:   time.Now(),
			Offset:      12,
			Successful:  true,
			Source:      account,
			SourceOwner: accountOwner,
			Dest:        dest,
			DestOwner:   destOwner,
			Quarks:      1235,
			MemoText:    &text,
			Memo:        nil,
			Subsidizer:  subsidizer,
		},
	}

	ownershipChanges := []*history.OwnershipChange{
		{
			Address: account,
			Owner:   account,
		},
		{
			Address: dest,
			Owner:   dest,
		},
	}

	assert.NoError(t, l.LoadData(history.StateChange{
		Creations:        creations,
		Payments:         payments,
		OwnershipChanges: ownershipChanges,
	}))

	require.Equal(t, 1, len(creationSubmitter.submitted))
	for i := 0; i < len(creations); i++ {
		submittedCreations := creationSubmitter.submitted[0].([]*history.Creation)
		expected := creations[i]
		if i == 1 {
			expected.AppIndex = 32
		}

		assert.EqualValues(t, expected, submittedCreations[i])
	}
	require.Equal(t, 1, len(paymentSubmitter.submitted))
	for i := 0; i < len(payments); i++ {
		submittedPayments := paymentSubmitter.submitted[0].([]*history.Payment)
		expected := payments[i]
		if i == 2 {
			expected.AppIndex = 32
		}

		assert.EqualValues(t, expected, submittedPayments[i])
	}
}

type submitter struct {
	sync.Mutex
	submitted []interface{}
}

func newTestSubmitter() *submitter {
	return &submitter{
		submitted: make([]interface{}, 0),
	}
}

func (s *submitter) Submit(ctx context.Context, src interface{}) (err error) {
	s.Lock()
	defer s.Unlock()

	s.submitted = append(s.submitted, src)
	return nil
}
