package solana

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/testutil"
	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
	historymemory "github.com/kinecosystem/agora/pkg/transaction/history/memory"
	"github.com/kinecosystem/agora/pkg/transaction/history/model"
)

type testEnv struct {
	client   *solana.MockClient
	writer   *historymemory.RW
	ingestor ingestion.Ingestor
	token    ed25519.PublicKey
}

func setup(t *testing.T) (env testEnv) {
	token := testutil.GenerateSolanaKeypair(t)

	env.token = token.Public().(ed25519.PublicKey)
	env.client = solana.NewMockClient()
	env.writer = historymemory.New()
	env.ingestor = New("test", env.client, env.token)
	return env
}

func TestInitialParams(t *testing.T) {
	env := setup(t)

	testCases := []struct {
		p    ingestion.Pointer
		slot uint64
	}{
		{p: nil, slot: 0},
		{p: pointerFromSlot(12), slot: 12},
	}

	for _, tc := range testCases {
		env.client.Calls = nil
		ctx, cancel := context.WithCancel(context.Background())

		env.client.On("GetBlockTime", mock.Anything).Return(time.Now(), nil)
		env.client.On("GetConfirmedBlocksWithLimit", mock.Anything, mock.Anything).Return([]uint64{}, nil)

		queue, err := env.ingestor.Ingest(ctx, env.writer, tc.p)
		require.NoError(t, err)

		// Ensure polling occurs
		for {
			env.client.Lock()
			n := len(env.client.Calls)
			env.client.Unlock()
			if n >= 2 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		cancel()

		var results int
		for resultCh := range queue {
			r := <-resultCh
			if r.Err == nil {
				results++
			}
		}
		assert.Equal(t, 0, results)
		require.True(t, len(env.client.Calls) > 0)

		// we may get multiple pollings, but all should be with expected slot
		for _, call := range env.client.Calls {
			assert.Equal(t, "GetConfirmedBlocksWithLimit", call.Method)
			assert.Equal(t, tc.slot+1, call.Arguments[0].(uint64))
			assert.Equal(t, uint64(1024), call.Arguments[1].(uint64))
		}
	}
}

func TestRoundTrip(t *testing.T) {
	env := setup(t)

	tokenAccount := token.Account{
		Mint: env.token,
	}
	accountInfo := solana.AccountInfo{
		Data:  tokenAccount.Marshal(),
		Owner: token.ProgramKey,
	}

	env.client.On("GetConfirmedBlocksWithLimit", uint64(1), mock.Anything).Return([]uint64{1, 2}, nil)
	env.client.On("GetConfirmedBlocksWithLimit", uint64(3), mock.Anything).Return([]uint64{3, 4}, nil)
	env.client.On("GetConfirmedBlocksWithLimit", uint64(5), mock.Anything).Return([]uint64{}, nil)
	env.client.On("GetBlockTime", mock.Anything).Return(time.Now(), nil)
	env.client.On("GetAccountInfo", mock.Anything, mock.Anything).Return(accountInfo, nil)

	blocks := generateBlocks(t, 4, 10)
	for i := 0; i < 4; i++ {
		env.client.On("GetConfirmedBlock", uint64(i+1)).Return(blocks[i], nil)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queue, err := env.ingestor.Ingest(ctx, env.writer, nil)
	require.NoError(t, err)

	var results []ingestion.Result
	for i := 0; i < len(blocks); i++ {
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

	// We've configured the ingestor to make multiple GetConfirmedBlocksWithLimit calls,
	// but due to the polling nature, we may see more. The lower bound, however, should be:
	//
	//  * 3 GetConfirmedBlocksWithLimit calls
	//  * 4 GetConfirmedBlock calls
	//
	assert.GreaterOrEqual(t, len(env.client.Calls), 7)

	assert.Nil(t, results[0].Err)
	for i := 1; i < len(results); i++ {
		assert.EqualValues(t, results[i].Parent, results[i-1].Block)
		assert.Nil(t, results[i].Err)
	}

	assert.Equal(t, 40, len(env.writer.Writes))
	written := make(map[string]struct{})
	for _, entry := range env.writer.Writes {
		solanaEntry, ok := entry.Kind.(*model.Entry_Solana)
		assert.True(t, ok)
		assert.Equal(t, model.KinVersion_KIN4, entry.Version)
		assert.True(t, solanaEntry.Solana.Confirmed)

		block := blocks[solanaEntry.Solana.Slot-1]
		entryTxn := solanaEntry.Solana.Transaction
		written[string(entryTxn)] = struct{}{}

		var found bool
		for _, txn := range block.Transactions {
			if bytes.Equal(txn.Transaction.Marshal(), entryTxn) {
				found = true
				break
			}
		}
		assert.True(t, found)
	}
	assert.Equal(t, 40, len(written))
}

func TestTransactionFilter(t *testing.T) {
	env := setup(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	senders := []ed25519.PrivateKey{
		testutil.GenerateSolanaKeypair(t),
		testutil.GenerateSolanaKeypair(t),
	}
	receivers := testutil.GenerateSolanaKeys(t, 2)
	otherToken := testutil.GenerateSolanaKeypair(t).Public().(ed25519.PublicKey)

	sourceAccounts := []token.Account{
		{
			Mint:   env.token,
			Owner:  senders[0].Public().(ed25519.PublicKey),
			Amount: 100,
			State:  token.AccountStateInitialized,
		},
		{
			Mint:   otherToken,
			Owner:  senders[1].Public().(ed25519.PublicKey),
			Amount: 100,
			State:  token.AccountStateInitialized,
		},
	}
	accountInfos := []solana.AccountInfo{
		{
			Owner: token.ProgramKey,
			Data:  sourceAccounts[0].Marshal(),
		},
		{
			Owner: token.ProgramKey,
			Data:  sourceAccounts[1].Marshal(),
		},
	}

	block := &solana.Block{
		Transactions: []solana.BlockTransaction{
			// For our mint
			{
				Transaction: solana.NewTransaction(
					senders[0].Public().(ed25519.PublicKey),
					token.Transfer(
						senders[0].Public().(ed25519.PublicKey),
						receivers[0],
						senders[0].Public().(ed25519.PublicKey),
						10,
					),
				),
			},
			// Another mint
			{
				Transaction: solana.NewTransaction(
					senders[1].Public().(ed25519.PublicKey),
					token.Transfer(
						senders[1].Public().(ed25519.PublicKey),
						receivers[0],
						senders[1].Public().(ed25519.PublicKey),
						10,
					),
				),
			},
			// Both mints
			{
				Transaction: solana.NewTransaction(
					senders[0].Public().(ed25519.PublicKey),
					token.Transfer(
						senders[0].Public().(ed25519.PublicKey),
						receivers[0],
						senders[0].Public().(ed25519.PublicKey),
						10,
					),
					token.Transfer(
						senders[1].Public().(ed25519.PublicKey),
						receivers[1],
						senders[1].Public().(ed25519.PublicKey),
						10,
					),
				),
			},
		},
	}

	for i, txn := range block.Transactions {
		assert.NoError(t, txn.Transaction.Sign(senders[i%2]))
	}

	env.client.On("GetConfirmedBlocksWithLimit", uint64(1), mock.Anything).Return([]uint64{1}, nil)
	env.client.On("GetConfirmedBlocksWithLimit", uint64(2), mock.Anything).Return([]uint64{}, nil)
	env.client.On("GetConfirmedBlock", uint64(1), mock.Anything).Return(block, nil)
	env.client.On("GetBlockTime", uint64(1)).Return(time.Now(), nil)
	env.client.On("GetAccountInfo", senders[0].Public().(ed25519.PublicKey), mock.Anything).Return(accountInfos[0], nil)
	env.client.On("GetAccountInfo", senders[1].Public().(ed25519.PublicKey), mock.Anything).Return(accountInfos[1], nil)

	queue, err := env.ingestor.Ingest(ctx, env.writer, nil)
	require.NoError(t, err)

	select {
	case r, ok := <-queue:
		if !ok {
			break
		}

		select {
		case <-r:
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for result")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for result (queue)")
	}

	expectedTransactions := map[string]struct{}{
		string(block.Transactions[0].Transaction.Marshal()): struct{}{},
		string(block.Transactions[2].Transaction.Marshal()): struct{}{},
	}
	actualTransactions := make(map[string]struct{})

	assert.Len(t, env.writer.Writes, 2)
	for _, entry := range env.writer.Writes {
		e := entry.GetSolana()

		_, exists := expectedTransactions[string(e.Transaction)]
		assert.True(t, exists)
		actualTransactions[string(e.Transaction)] = struct{}{}
	}

	assert.Len(t, actualTransactions, 2)
}

func generateBlocks(t *testing.T, num, txnsPerBlock int) []*solana.Block {
	var blocks []*solana.Block

	for i := 0; i < num; i++ {
		b := &solana.Block{
			ParentSlot: uint64(i),
			Slot:       uint64(i) + 1,
		}
		if i > 0 {
			b.PrevHash = blocks[i-1].Hash
		}

		h := sha256.New()
		for j := 0; j < txnsPerBlock; j++ {
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

			b.Transactions = append(b.Transactions, solana.BlockTransaction{
				Transaction: txn,
			})
			_, err := h.Write(txn.Signature())
			assert.NoError(t, err)
		}

		b.Hash = h.Sum(nil)
		blocks = append(blocks, b)
	}

	return blocks
}
