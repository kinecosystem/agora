package solana

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ed25519"

	commonpbv3 "github.com/kinecosystem/agora-api/genproto/common/v3"
	commonpbv4 "github.com/kinecosystem/agora-api/genproto/common/v4"
	transactionpb "github.com/kinecosystem/agora-api/genproto/transaction/v4"

	"github.com/kinecosystem/agora/pkg/invoice"
	invoicedb "github.com/kinecosystem/agora/pkg/invoice/memory"
	"github.com/kinecosystem/agora/pkg/testutil"
	"github.com/kinecosystem/agora/pkg/transaction/history"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
	ingestionmemory "github.com/kinecosystem/agora/pkg/transaction/history/ingestion/memory"
	historymemory "github.com/kinecosystem/agora/pkg/transaction/history/memory"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
	historytestutil "github.com/kinecosystem/agora/pkg/transaction/history/model/testutil"
)

type testEnv struct {
	client       *solana.MockClient
	rw           history.ReaderWriter
	committer    ingestion.Committer
	invoiceStore invoice.Store
	loader       *loader
}

func setup(t *testing.T) (env testEnv) {
	env.client = solana.NewMockClient()
	env.rw = historymemory.New()
	env.committer = ingestionmemory.New()
	env.invoiceStore = invoicedb.New()
	env.loader = newLoader(
		env.client,
		env.rw,
		env.committer,
		env.invoiceStore,
		testutil.GenerateSolanaKeys(t, 1)[0],
	)

	return env
}

func TestLoadTransaction_Stellar(t *testing.T) {
	env := setup(t)

	il, _, _ := generateInvoice(t, 1)

	accounts := testutil.GenerateAccountIDs(t, 2)
	entry, hash := historytestutil.GenerateStellarEntry(t, 1, 2, accounts[0], accounts[1:], nil, nil)

	require.NoError(t, env.rw.Write(context.Background(), entry))
	require.NoError(t, env.invoiceStore.Put(context.Background(), hash, il))

	resp, err := env.loader.loadTransaction(context.Background(), hash)
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.GetTransactionResponse_SUCCESS, resp.State)
	assert.EqualValues(t, 0, resp.Slot)
	assert.EqualValues(t, 0, resp.Confirmations)

	assert.True(t, proto.Equal(il, resp.Item.InvoiceList))
	assert.Equal(t, entry.GetStellar().EnvelopeXdr, resp.Item.GetStellarTransaction().EnvelopeXdr)
	assert.Equal(t, entry.GetStellar().ResultXdr, resp.Item.GetStellarTransaction().ResultXdr)
	assert.Nil(t, resp.Item.TransactionError)
	assert.True(t, proto.Equal(&commonpbv4.TransactionId{Value: hash}, resp.Item.TransactionId))

	var envelope xdr.TransactionEnvelope
	assert.NoError(t, envelope.UnmarshalBinary(entry.GetStellar().EnvelopeXdr))
	expected, err := paymentsFromEnvelope(envelope)
	assert.NoError(t, err)

	assert.Equal(t, len(expected), len(resp.Item.Payments))
	for i := 0; i < len(expected); i++ {
		assert.True(t, proto.Equal(expected[i], resp.Item.Payments[i]))
	}

	k, err := entry.GetOrderingKey()
	assert.NoError(t, err)
	assert.Equal(t, k, resp.Item.Cursor.Value)
}

func TestLoadTransaction_FromHistory(t *testing.T) {
	env := setup(t)

	invoice, invoiceHash, _ := generateInvoice(t, 1)
	sender := testutil.GenerateSolanaKeypair(t)
	receivers := testutil.GenerateSolanaKeys(t, 1)
	entry, hash := historytestutil.GenerateSolanaEntry(t, 1, true, sender, receivers, invoiceHash, nil)

	require.NoError(t, env.rw.Write(context.Background(), entry))
	require.NoError(t, env.invoiceStore.Put(context.Background(), hash, invoice))

	resp, err := env.loader.loadTransaction(context.Background(), hash)
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.GetTransactionResponse_SUCCESS, resp.State)
	assert.EqualValues(t, 1, resp.Slot)
	assert.EqualValues(t, 0, resp.Confirmations)

	assert.True(t, proto.Equal(invoice, resp.Item.InvoiceList))
	assert.Equal(t, entry.GetSolana().Transaction, resp.Item.GetSolanaTransaction().Value)
	assert.Nil(t, resp.Item.TransactionError)
	assert.True(t, proto.Equal(&commonpbv4.TransactionId{Value: hash}, resp.Item.TransactionId))

	var txn solana.Transaction
	assert.NoError(t, txn.Unmarshal(entry.GetSolana().Transaction))
	expected := paymentsFromTransaction(txn)

	assert.Equal(t, len(expected), len(resp.Item.Payments))
	for i := 0; i < len(expected); i++ {
		assert.True(t, proto.Equal(expected[i], resp.Item.Payments[i]))
	}

	k, err := entry.GetOrderingKey()
	assert.NoError(t, err)
	assert.Equal(t, k, resp.Item.Cursor.Value)
}

func TestLoadTransaction_FromClient(t *testing.T) {
	env := setup(t)

	invoice, invoiceHash, _ := generateInvoice(t, 1)
	sender := testutil.GenerateSolanaKeypair(t)
	receivers := testutil.GenerateSolanaKeys(t, 1)
	entry, hash := historytestutil.GenerateSolanaEntry(t, 10, true, sender, receivers, invoiceHash, nil)

	require.NoError(t, env.invoiceStore.Put(context.Background(), hash, invoice))

	txID, err := entry.GetTxID()
	assert.NoError(t, err)
	var sig solana.Signature
	copy(sig[:], txID)

	var txn solana.Transaction
	assert.NoError(t, txn.Unmarshal(entry.GetSolana().Transaction))
	confirmedTransaction := solana.ConfirmedTransaction{
		Slot:        10,
		Transaction: txn,
	}

	env.client.On("GetConfirmedTransaction", sig).Return(confirmedTransaction, nil)

	resp, err := env.loader.loadTransaction(context.Background(), hash)
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.GetTransactionResponse_SUCCESS, resp.State)
	assert.EqualValues(t, 10, resp.Slot)
	assert.EqualValues(t, 0, resp.Confirmations)

	assert.True(t, proto.Equal(invoice, resp.Item.InvoiceList))
	assert.Equal(t, entry.GetSolana().Transaction, resp.Item.GetSolanaTransaction().Value)
	assert.Nil(t, resp.Item.TransactionError)
	assert.True(t, proto.Equal(&commonpbv4.TransactionId{Value: hash}, resp.Item.TransactionId))

	expected := paymentsFromTransaction(txn)

	assert.Equal(t, len(expected), len(resp.Item.Payments))
	for i := 0; i < len(expected); i++ {
		assert.True(t, proto.Equal(expected[i], resp.Item.Payments[i]))
	}

	k, err := entry.GetOrderingKey()
	assert.NoError(t, err)
	assert.Equal(t, k, resp.Item.Cursor.Value)
}

func TestLoadTransaction_Failed(t *testing.T) {
	env := setup(t)

	invoice, invoiceHash, _ := generateInvoice(t, 1)
	sender := testutil.GenerateSolanaKeypair(t)
	receivers := testutil.GenerateSolanaKeys(t, 1)
	entry, hash := historytestutil.GenerateSolanaEntry(t, 1, true, sender, receivers, invoiceHash, nil)

	_, err := env.rw.GetTransaction(context.Background(), hash)
	assert.Equal(t, history.ErrNotFound, err)

	txErr := solana.NewTransactionError(solana.TransactionErrorAccountNotFound)
	raw, err := txErr.JSONString()
	assert.NoError(t, err)
	entry.GetSolana().TransactionError = []byte(raw)

	require.NoError(t, env.rw.Write(context.Background(), entry))
	require.NoError(t, env.invoiceStore.Put(context.Background(), hash, invoice))

	resp, err := env.loader.loadTransaction(context.Background(), hash)
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.GetTransactionResponse_FAILED, resp.State)
	assert.EqualValues(t, 1, resp.Slot)
	assert.EqualValues(t, 0, resp.Confirmations)

	assert.True(t, proto.Equal(invoice, resp.Item.InvoiceList))
	assert.Equal(t, entry.GetSolana().Transaction, resp.Item.GetSolanaTransaction().Value)
	assert.NotNil(t, resp.Item.TransactionError)
	assert.Equal(t, commonpbv4.TransactionError_INVALID_ACCOUNT, resp.Item.TransactionError.Reason)
	assert.Equal(t, raw, string(resp.Item.TransactionError.Raw))
	assert.True(t, proto.Equal(&commonpbv4.TransactionId{Value: hash}, resp.Item.TransactionId))

	// Writeback should have occurred
	_, err = env.rw.GetTransaction(context.Background(), hash)
	assert.NoError(t, err)
}

func TestLoadTransaction_UpgradeConfirmed(t *testing.T) {
	env := setup(t)

	invoice, invoiceHash, _ := generateInvoice(t, 1)
	sender := testutil.GenerateSolanaKeypair(t)
	receivers := testutil.GenerateSolanaKeys(t, 1)
	entry, hash := historytestutil.GenerateSolanaEntry(t, 1, false, sender, receivers, invoiceHash, nil)

	require.NoError(t, env.rw.Write(context.Background(), entry))
	require.NoError(t, env.invoiceStore.Put(context.Background(), hash, invoice))

	txID, err := entry.GetTxID()
	assert.NoError(t, err)
	var sig solana.Signature
	copy(sig[:], txID)

	confirmations := 10
	status := &solana.SignatureStatus{
		Slot:          1,
		Confirmations: &confirmations,
	}

	env.client.On("GetSignatureStatus", sig, mock.Anything).Return(status, nil)

	resp, err := env.loader.loadTransaction(context.Background(), hash)
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.GetTransactionResponse_PENDING, resp.State)
	assert.EqualValues(t, 1, resp.Slot)
	assert.EqualValues(t, 10, resp.Confirmations)

	// upgrade to success
	status.Confirmations = nil

	resp, err = env.loader.loadTransaction(context.Background(), hash)
	assert.NoError(t, err)
	assert.Equal(t, transactionpb.GetTransactionResponse_SUCCESS, resp.State)
	assert.EqualValues(t, 1, resp.Slot)
	assert.EqualValues(t, 0, resp.Confirmations)
}

func TestGetItems_Query(t *testing.T) {
	env := setup(t)

	sender := testutil.GenerateSolanaKeypair(t)
	var senderPubKey xdr.Uint256
	copy(senderPubKey[:], sender.Public().(ed25519.PublicKey))
	senderAccount := xdr.AccountId{
		Type:    xdr.PublicKeyTypePublicKeyTypeEd25519,
		Ed25519: &senderPubKey,
	}
	accounts := testutil.GenerateAccountIDs(t, 10)

	// Since we always pull history from the identity account, it's ok
	// that we haven't mapped any of accounts here. We test separately
	// to ensure account resolution works for solana.
	env.client.On("GetTokenAccountsByOwner", mock.Anything, env.loader.token).Return([]ed25519.PublicKey{}, nil)

	generated := make([]*model.Entry, 40)
	for i := range generated {
		if i < 20 {
			generated[i], _ = historytestutil.GenerateStellarEntry(t, uint64(i-i%2), i, senderAccount, accounts[1:], nil, nil)
		} else {
			receivers := testutil.GenerateSolanaKeys(t, 10)
			generated[i], _ = historytestutil.GenerateSolanaEntry(t, uint64(i), true, sender, receivers, nil, nil)
		}

		assert.NoError(t, env.rw.Write(context.Background(), generated[i]))
	}

	// Request history from beginning, but without any committed entries.
	items, err := env.loader.getItems(context.Background(), senderPubKey[:], nil, transactionpb.GetHistoryRequest_ASC)
	assert.NoError(t, err)
	assert.Empty(t, items)

	// Advance to the 5th entry
	require.NoError(t, env.committer.Commit(context.Background(), ingestion.GetHistoryIngestorName(model.KinVersion_KIN3), nil, historytestutil.GetOrderingKey(t, generated[4])))
	items, err = env.loader.getItems(context.Background(), senderPubKey[:], nil, transactionpb.GetHistoryRequest_ASC)
	assert.NoError(t, err)
	assert.Len(t, items, 5)

	// Request in reverse order to ensure we're trimming correctly.
	items, err = env.loader.getItems(context.Background(), senderPubKey[:], nil, transactionpb.GetHistoryRequest_DESC)
	assert.NoError(t, err)
	assert.Len(t, items, 5)

	// Mark all as committed
	latest := historytestutil.GetOrderingKey(t, generated[len(generated)-1])
	require.NoError(t, env.committer.Commit(context.Background(), ingestion.GetHistoryIngestorName(model.KinVersion_KIN4), nil, latest))

	testCases := []struct {
		direction transactionpb.GetHistoryRequest_Direction
		start     int
		expected  []*model.Entry
	}{
		{
			start:    -1,
			expected: generated,
		},
		{
			start: 10,
			// Since we don't include the entry at the cursor position,
			// we should receive cursor+1 and onward.
			expected: generated[11:],
		},
		{
			start:     -1,
			direction: transactionpb.GetHistoryRequest_DESC,
			// Since we haven't specified a cursor, the loader should
			// default to the _latest_ entry as a cursor.
			//
			// However, unlike when we explicitly set a cursor, we _do_
			// want to include the value at the cursor, as the client
			// does not know about it yet.
			expected: generated,
		},
		{
			start:     1,
			direction: transactionpb.GetHistoryRequest_DESC,
			expected:  generated[0:1],
		},
		{
			start:     len(generated) - 1,
			direction: transactionpb.GetHistoryRequest_DESC,
			expected:  generated[:len(generated)-1],
		},
	}

	for i, tc := range testCases {
		var cursor *transactionpb.Cursor
		if tc.start >= 0 {
			k, err := generated[tc.start].GetOrderingKey()
			require.NoError(t, err)
			cursor = &transactionpb.Cursor{
				Value: k,
			}
		}

		items, err = env.loader.getItems(context.Background(), senderPubKey[:], cursor, tc.direction)
		assert.NoError(t, err)
		assert.Equal(t, len(tc.expected), len(items), "case: %d", i)

		for i := 0; i < len(tc.expected); i++ {
			var expected *transactionpb.HistoryItem
			if tc.direction == transactionpb.GetHistoryRequest_ASC {
				expected, err = historyItemFromEntry(tc.expected[i])
			} else {
				expected, err = historyItemFromEntry(tc.expected[len(tc.expected)-1-i])
			}
			require.NoError(t, err)

			assert.True(t, proto.Equal(expected, items[i]))
		}
	}
}

func TestGetItems_Invoices(t *testing.T) {
	env := setup(t)

	sender := testutil.GenerateSolanaKeypair(t)
	var senderPubKey xdr.Uint256
	copy(senderPubKey[:], sender.Public().(ed25519.PublicKey))
	senderAccount := xdr.AccountId{
		Type:    xdr.PublicKeyTypePublicKeyTypeEd25519,
		Ed25519: &senderPubKey,
	}
	accounts := testutil.GenerateAccountIDs(t, 10)

	generated := make([]*model.Entry, 40)
	invoices := make([]*commonpbv3.InvoiceList, 40)
	for i := range generated {
		var id []byte
		var invoiceListHash []byte
		if i%2 == 0 {
			invoices[i] = &commonpbv3.InvoiceList{
				Invoices: []*commonpbv3.Invoice{
					{
						Items: []*commonpbv3.Invoice_LineItem{
							{
								Title:       "test",
								Description: "desc",
								Amount:      int64(i),
							},
						},
					},
				},
			}
			b, err := proto.Marshal(invoices[i])
			require.NoError(t, err)
			h := sha256.Sum224(b)
			invoiceListHash = h[:]
		}

		if i < 20 {
			generated[i], id = historytestutil.GenerateStellarEntry(t, uint64(i-i%2), i, senderAccount, accounts[1:], invoiceListHash, nil)
		} else {
			receivers := testutil.GenerateSolanaKeys(t, 10)
			generated[i], id = historytestutil.GenerateSolanaEntry(t, uint64(i), true, sender, receivers, invoiceListHash, nil)
		}

		if i%2 == 0 {
			assert.NoError(t, env.invoiceStore.Put(context.Background(), id, invoices[i]))
		}

		assert.NoError(t, env.rw.Write(context.Background(), generated[i]))
	}

	// Since we always pull history from the identity account, it's ok
	// that we haven't mapped any of accounts here. We test separately
	// to ensure account resolution works for solana.
	env.client.On("GetTokenAccountsByOwner", mock.Anything, env.loader.token).Return([]ed25519.PublicKey{}, nil)

	latest := historytestutil.GetOrderingKey(t, generated[len(generated)-1])
	require.NoError(t, env.committer.Commit(context.Background(), ingestion.GetHistoryIngestorName(model.KinVersion_KIN4), nil, latest))

	entries, err := env.loader.getItems(context.Background(), senderPubKey[:], nil, transactionpb.GetHistoryRequest_ASC)
	assert.NoError(t, err)
	assert.Len(t, entries, 40)

	for i := range entries {
		var expected *transactionpb.HistoryItem
		expected, err = historyItemFromEntry(generated[i])
		require.NoError(t, err)
		expected.InvoiceList = invoices[i]

		assert.True(t, proto.Equal(expected, entries[i]))
	}
}

func TestGetEntriesForAccount(t *testing.T) {
	env := setup(t)

	sender := testutil.GenerateSolanaKeypair(t)
	senderB := testutil.GenerateSolanaKeypair(t)
	var senderPubKey xdr.Uint256
	copy(senderPubKey[:], sender.Public().(ed25519.PublicKey))
	senderAccount := xdr.AccountId{
		Type:    xdr.PublicKeyTypePublicKeyTypeEd25519,
		Ed25519: &senderPubKey,
	}
	accounts := testutil.GenerateAccountIDs(t, 10)

	// We return identity, as well as one of the overlapping transaction
	// accounts to ensure we filter duplicate resolved addresses, as well
	// as duplicate entries correctly.
	resolvedAddresses := []ed25519.PublicKey{
		sender.Public().(ed25519.PublicKey),
		senderB.Public().(ed25519.PublicKey),
	}

	env.client.On("GetTokenAccountsByOwner", mock.Anything, env.loader.token).Return(resolvedAddresses, nil)

	// Generate:
	//   1. Stellar history
	//   2. Solana sender history, interleaved with;
	//   3. Solana sender B history, as well as;
	//   4. Solana sender A to sender B transactions
	generated := make([]*model.Entry, 80)
	for i := 0; i < 20; i++ {
		generated[i], _ = historytestutil.GenerateStellarEntry(t, uint64(i-i%2), i, senderAccount, accounts[1:], nil, nil)
	}
	for i := 20; i < 40; i++ {
		receivers := testutil.GenerateSolanaKeys(t, 10)
		generated[i], _ = historytestutil.GenerateSolanaEntry(t, uint64(i), true, sender, receivers, nil, nil)
		generated[i+20], _ = historytestutil.GenerateSolanaEntry(t, uint64(i+20), true, senderB, receivers, nil, nil)
	}
	for i := 60; i < 80; i++ {
		generated[i], _ = historytestutil.GenerateSolanaEntry(t, uint64(i), true, sender, []ed25519.PublicKey{senderB.Public().(ed25519.PublicKey)}, nil, nil)
	}

	// We add and verify in pieces to ensure edge cases work ok.
	ranges := []struct {
		start int
		end   int
	}{
		{
			// stellar only
			start: 0,
			end:   20,
		},
		{
			// stellar + merge(sender, senderA)
			start: 20,
			end:   60,
		},
		{
			// stellar + merge(sender, senderA) + overlapped(sender, senderA) (edge case of 1)
			start: 60,
			end:   61,
		},
		{
			// stellar + merge(sender, senderA) + overlapped(sender, senderA)
			start: 61,
			end:   80,
		},
	}

	for _, r := range ranges {
		for i := r.start; i < r.end; i++ {
			assert.NoError(t, env.rw.Write(context.Background(), generated[i]), "%d", i)
		}

		entries, err := env.loader.getEntriesForAccount(context.Background(), sender, &history.ReadOptions{})
		assert.NoError(t, err)
		assert.Equal(t, r.end, len(entries))

		for i := 0; i < len(entries); i++ {
			assert.True(t, proto.Equal(generated[i], entries[i]))
		}
	}

	// Ensure that the function respects the order, limit, and start
	// position, even after merging.
	//
	// note: we compute the start based on ordering. more start selection
	//       situations is really handled by the calling method, getItems()
	testCases := []struct {
		opts     *history.ReadOptions
		expected []*model.Entry
	}{
		{
			opts:     &history.ReadOptions{},
			expected: generated,
		},
		{
			opts: &history.ReadOptions{
				Descending: true,
			},
			expected: generated,
		},
		{
			opts: &history.ReadOptions{
				Limit: 40,
			},
			expected: generated[:40],
		},
		{
			opts: &history.ReadOptions{
				Descending: true,
				Limit:      40,
			},
			expected: generated[40:],
		},
	}

	for i, tc := range testCases {
		if tc.opts.Descending {
			k, err := generated[len(generated)-1].GetOrderingKey()
			assert.NoError(t, err)
			tc.opts.Start = k
		}

		entries, err := env.loader.getEntriesForAccount(context.Background(), sender, tc.opts)
		assert.NoError(t, err)
		assert.Equal(t, len(tc.expected), len(entries), "%d", i)
	}
}

func TestPaymentsFromEnvelope(t *testing.T) {
	accounts := testutil.GenerateAccountIDs(t, 6)
	entry, _ := historytestutil.GenerateStellarEntry(t, 1, 2, accounts[0], accounts[1:], nil, nil)

	var envelope xdr.TransactionEnvelope
	assert.NoError(t, envelope.UnmarshalBinary(entry.GetStellar().EnvelopeXdr))

	payments, err := paymentsFromEnvelope(envelope)
	assert.NoError(t, err)
	assert.Len(t, payments, 5)

	for i, p := range payments {
		sender := (*accounts[0].Ed25519)[:]
		dest := (*accounts[i+1].Ed25519)[:]
		assert.EqualValues(t, sender, p.Source.Value)
		assert.EqualValues(t, dest, p.Destination.Value)
		assert.EqualValues(t, 10, p.Amount)
		assert.EqualValues(t, i, p.Index)
	}
}

func TestPaymentsFromTransaction(t *testing.T) {
	sender := testutil.GenerateSolanaKeypair(t)
	receivers := testutil.GenerateSolanaKeys(t, 5)
	entry, _ := historytestutil.GenerateSolanaEntry(t, 10, true, sender, receivers, nil, nil)

	var txn solana.Transaction
	assert.NoError(t, txn.Unmarshal(entry.GetSolana().Transaction))

	payments := paymentsFromTransaction(txn)
	assert.Len(t, payments, 5)

	for i, p := range payments {
		assert.EqualValues(t, sender.Public().(ed25519.PublicKey), p.Source.Value)
		assert.EqualValues(t, receivers[i], p.Destination.Value)
		assert.EqualValues(t, i+10+1, p.Amount)
		assert.EqualValues(t, i, p.Index)
	}
}

func generateInvoice(t *testing.T, entries int) (*commonpbv3.InvoiceList, []byte, []byte) {
	invoiceList := &commonpbv3.InvoiceList{}
	for i := 0; i < entries; i++ {
		invoiceList.Invoices = append(invoiceList.Invoices, &commonpbv3.Invoice{
			Items: []*commonpbv3.Invoice_LineItem{
				{
					Title:       "line item",
					Description: "desc",
					Amount:      int64(i + 1),
				},
			},
		})
	}
	bytes, err := proto.Marshal(invoiceList)
	require.NoError(t, err)

	h := sha256.Sum224(bytes)

	return invoiceList, h[:], bytes
}
