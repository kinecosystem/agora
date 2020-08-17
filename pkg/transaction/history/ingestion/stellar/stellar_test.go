package stellar

import (
	"context"
	"encoding/base64"
	"strconv"
	"testing"
	"time"

	"github.com/stellar/go/clients/horizonclient"
	hProtocol "github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/testutil"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
	historymemory "github.com/kinecosystem/agora/pkg/transaction/history/memory"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

type testEnv struct {
	horizonClient *horizonclient.MockClient
	writer        *historymemory.RW
	ingestor      ingestion.Ingestor
}

func setup(t *testing.T) (env testEnv) {
	env.horizonClient = &horizonclient.MockClient{}
	env.writer = historymemory.New()
	env.ingestor = New("test", model.KinVersion_KIN3, env.horizonClient, "network passphrase")

	return env
}

func TestInitialParams(t *testing.T) {
	env := setup(t)

	testCases := []struct {
		p ingestion.Pointer
		c string
	}{
		{p: nil, c: "0"},
		{p: pointerFromSequence(model.KinVersion_KIN3, 1024), c: "4398046511104"},
	}

	for _, tc := range testCases {
		env.horizonClient.Calls = nil

		cancelCh := make(chan time.Time)
		ctx, cancel := context.WithCancel(context.Background())
		env.horizonClient.On("StreamLedgers", mock.Anything, mock.Anything, mock.Anything).WaitUntil(cancelCh).Return(nil)

		queue, err := env.ingestor.Ingest(ctx, env.writer, tc.p)
		require.NoError(t, err)
		cancel()
		close(cancelCh)

		var results int
		for resultCh := range queue {
			r := <-resultCh
			if r.Err == nil {
				results++
			}
		}
		assert.Equal(t, 0, results)
		require.Len(t, env.horizonClient.Calls, 1)

		call := env.horizonClient.Calls[0]
		assert.Equal(t, "StreamLedgers", call.Method)
		assert.Equal(t, tc.c, call.Arguments[1].(horizonclient.LedgerRequest).Cursor)
		assert.Equal(t, horizonclient.OrderAsc, call.Arguments[1].(horizonclient.LedgerRequest).Order)
	}
}

func TestRoundTrip(t *testing.T) {
	env := setup(t)

	ledgers := generateLedgers(t, 5, 3)
	for _, l := range ledgers {
		requests := []horizonclient.TransactionRequest{
			{
				ForLedger: uint(l.ledger.Sequence),
				Order:     horizonclient.OrderAsc,
				Limit:     200,
			},
			{
				ForLedger: uint(l.ledger.Sequence),
				Order:     horizonclient.OrderAsc,
				Limit:     200,
				Cursor:    l.txnPage.Embedded.Records[1].PagingToken(),
			},
			{
				ForLedger: uint(l.ledger.Sequence),
				Order:     horizonclient.OrderAsc,
				Limit:     200,
				Cursor:    l.txnPage.Embedded.Records[2].PagingToken(),
			},
		}

		// splice the txnPage in half to emulate paging
		pages := []hProtocol.TransactionsPage{
			{
				Embedded: struct{ Records []hProtocol.Transaction }{
					Records: l.txnPage.Embedded.Records[0:2],
				},
			},
			{
				Embedded: struct{ Records []hProtocol.Transaction }{
					Records: l.txnPage.Embedded.Records[2:3],
				},
			},
			{
				Embedded: struct{ Records []hProtocol.Transaction }{},
			},
		}

		for i := 0; i < len(requests); i++ {
			env.horizonClient.On("Transactions", requests[i]).Return(pages[i], nil)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	emitter := func(args mock.Arguments) {
		h := args[2].(horizonclient.LedgerHandler)

		for _, l := range ledgers {
			h(l.ledger)
		}

		<-ctx.Done()
	}

	env.horizonClient.
		On("StreamLedgers", mock.Anything, mock.Anything, mock.Anything).
		Run(emitter).
		Return(nil)

	queue, err := env.ingestor.Ingest(ctx, env.writer, nil)
	require.NoError(t, err)

	var results []ingestion.Result
	for i := 0; i < len(ledgers); i++ {
		select {
		case r, ok := <-queue:
			if !ok {
				break
			}

			select {
			case result := <-r:
				results = append(results, result)
			case <-time.After(10 * time.Second):
				t.Fatal("timed out waiting for result")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for result (queue)")
		}
	}

	// We've forced the horizon client to break up each ledger into 2 pages.
	// Since the ingestor keeps paging until it sees empty results, we expect
	// that each ledger takes N+1 requests (for N pages).
	//
	// Additionally, there is a StreamLedgers call, making the total calls to
	// horizon, StreamLedgers(1) + Transactions(5*3)
	assert.Len(t, env.horizonClient.Calls, 1+5*3)

	assert.Nil(t, results[0].Err)
	for i := 1; i < len(results); i++ {
		assert.EqualValues(t, results[i].Parent, results[i-1].Block)
		assert.Nil(t, results[i].Err)
	}

	assert.Len(t, env.writer.Writes, 15)
	written := make(map[string]struct{})
	for _, entry := range env.writer.Writes {
		stellarEntry, ok := entry.Kind.(*model.Entry_Stellar)
		assert.True(t, ok)
		assert.Equal(t, model.KinVersion_KIN3, entry.Version)

		page := ledgers[stellarEntry.Stellar.Ledger]
		envelope := base64.StdEncoding.EncodeToString(stellarEntry.Stellar.EnvelopeXdr)
		written[envelope] = struct{}{}

		var found bool
		for _, txn := range page.txnPage.Embedded.Records {
			if txn.EnvelopeXdr == envelope {
				assert.Equal(t, base64.StdEncoding.EncodeToString(stellarEntry.Stellar.ResultXdr), txn.ResultXdr)
				found = true
				break
			}
		}
		assert.True(t, found)
	}
	assert.Len(t, written, 15)
}

type Ledger struct {
	ledger  hProtocol.Ledger
	txnPage hProtocol.TransactionsPage
}

func generateLedgers(t *testing.T, num, txnsPerLedger int) []Ledger {
	var ledgers []Ledger

	startTime := time.Now()
	for i := 0; i < num; i++ {
		l := Ledger{
			ledger: hProtocol.Ledger{
				Sequence: int32(i),
				ClosedAt: startTime.Add(5 * time.Second),
			},
		}
		for j := 0; j < txnsPerLedger; j++ {
			_, src := testutil.GenerateAccountID(t)
			_, dst := testutil.GenerateAccountID(t)
			op := testutil.GeneratePaymentOperation(&src, dst)
			opResult := xdr.OperationResult{
				Code: xdr.OperationResultCodeOpInner,
				Tr: &xdr.OperationResultTr{
					Type: xdr.OperationTypePayment,
					PaymentResult: &xdr.PaymentResult{
						Code: xdr.PaymentResultCodePaymentSuccess,
					},
				},
			}

			envelopeBytes, err := testutil.GenerateTransactionEnvelope(src, i+j, []xdr.Operation{op}).MarshalBinary()
			require.NoError(t, err)
			resultBytes, err := testutil.GenerateTransactionResult(xdr.TransactionResultCodeTxSuccess, []xdr.OperationResult{opResult}).MarshalBinary()
			require.NoError(t, err)

			txn := hProtocol.Transaction{
				EnvelopeXdr: base64.StdEncoding.EncodeToString(envelopeBytes),
				ResultXdr:   base64.StdEncoding.EncodeToString(resultBytes),
				PT:          strconv.FormatUint(uint64(i)<<32|uint64(j)<<12, 10),
			}
			l.txnPage.Embedded.Records = append(l.txnPage.Embedded.Records, txn)
		}
		ledgers = append(ledgers, l)
	}

	return ledgers
}
