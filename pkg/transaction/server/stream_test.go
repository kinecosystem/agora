package server

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/events/eventspb"
	"github.com/kinecosystem/agora/pkg/solanautil"
	"github.com/kinecosystem/agora/pkg/testutil"
)

func TestOpenTransactionStream(t *testing.T) {
	client := solana.NewMockClient()
	ctx, cancel := context.WithCancel(context.Background())
	notifier := newTestNotifier(3*10, cancel)

	// Stream Flow:
	//   1. GetSlot(solana.CommitmentMax) for initial seed position.
	//   2. blocks := GetConfirmedBlocksWithLimit(seed, 1024)
	//   3. seed = last(block)
	// loop:
	//   4. blocks: GetConfirmedBlocksWithLimit(seed, 1024)
	//   5. for b in B: GetConfirmedBlock(b)

	client.On("GetSlot", solana.CommitmentMax).Return(uint64(70), nil)
	client.On("GetConfirmedBlocksWithLimit", uint64(70), uint64(1024)).Return([]uint64{70, 71, 72}, nil)
	client.On("GetConfirmedBlocksWithLimit", uint64(72), uint64(1024)).Return([]uint64{72, 73, 74}, nil)
	client.On("GetConfirmedBlocksWithLimit", uint64(75), uint64(1024)).Return([]uint64{}, nil)

	blocks := generateBlocks(t, 3, 10)
	for i := 0; i < len(blocks); i++ {
		client.On("GetConfirmedBlock", uint64(i+72)).Return(blocks[i], nil)
	}

	go func() {
		StreamTransactions(ctx, client, notifier)
	}()

	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
	}

	assert.Equal(t, len(notifier.receivedTxns), 30)

	fmt.Println("running")

	for i, txn := range notifier.receivedTxns {
		expected := blocks[i/10].Transactions[i%10]
		require.EqualValues(t, expected, txn, i)
	}
}

func TestMapTransactionEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	notifier := newTestNotifier(10, cancel)

	block := generateBlocks(t, 1, 10)[0]
	for i := 0; i < len(block.Transactions); i++ {
		e := &eventspb.Event{
			SubmissionTime: ptypes.TimestampNow(),
			Kind: &eventspb.Event_TransactionEvent{
				TransactionEvent: &eventspb.TransactionEvent{
					Transaction: block.Transactions[i].Transaction.Marshal(),
				},
			},
		}
		if i >= 5 {
			txErr := solana.NewTransactionError(solana.TransactionErrorDuplicateSignature)
			str, err := txErr.JSONString()
			require.NoError(t, err)

			e.GetTransactionEvent().TransactionError = []byte(str)
		}

		MapTransactionEvent(notifier)(e)
	}

	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
	}

	require.Equal(t, 10, len(notifier.receivedTxns))
	for i := 0; i < 5; i++ {
		assert.Equal(t, block.Transactions[i], notifier.receivedTxns[i])
	}
	for i := 5; i < 10; i++ {
		assert.Equal(t, block.Transactions[i].Transaction, notifier.receivedTxns[i].Transaction)
		assert.True(t, solanautil.IsDuplicateSignature(notifier.receivedTxns[i].Err))
	}
}

func TestMapTransactionEvent_Invalid(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	notifier := newTestNotifier(1, cancel)

	block := generateBlocks(t, 1, 2)[0]
	for i := 0; i < 2; i++ {
		e := &eventspb.Event{
			SubmissionTime: ptypes.TimestampNow(),
			Kind: &eventspb.Event_TransactionEvent{
				TransactionEvent: &eventspb.TransactionEvent{
					Transaction: block.Transactions[i].Transaction.Marshal(),
				},
			},
		}
		if i == 0 {
			e.GetTransactionEvent().TransactionError = []byte("garbgarb")
		}

		MapTransactionEvent(notifier)(e)
	}

	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
	}

	require.Equal(t, 1, len(notifier.receivedTxns))
	assert.Equal(t, block.Transactions[1], notifier.receivedTxns[0])
}

type testNotifier struct {
	sync.Mutex
	receivedTxns []solana.BlockTransaction
	count        int
	cancel       context.CancelFunc
}

func newTestNotifier(count int, cancel context.CancelFunc) *testNotifier {
	return &testNotifier{
		count:  count,
		cancel: cancel,
	}
}

func (n *testNotifier) OnTransaction(b solana.BlockTransaction) {
	n.Lock()
	defer n.Unlock()

	n.receivedTxns = append(n.receivedTxns, b)
	if len(n.receivedTxns) == n.count {
		n.cancel()
	}
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
