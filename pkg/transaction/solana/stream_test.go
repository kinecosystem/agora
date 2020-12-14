package solana

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/kinecosystem/agora-common/solana"
	"github.com/kinecosystem/agora-common/solana/token"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
