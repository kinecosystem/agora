package tests

import (
	"context"
	"testing"

	"github.com/kinecosystem/agora-common/solana"
	"github.com/stretchr/testify/assert"

	"github.com/kinecosystem/agora/pkg/account"
	"github.com/kinecosystem/agora/pkg/testutil"
)

func RunTests(t *testing.T, m account.Mapper, teardown func()) {
	for _, tf := range []func(t *testing.T, m account.Mapper){
		testRoundTrip,
	} {
		tf(t, m)
		teardown()
	}
}

func testRoundTrip(t *testing.T, m account.Mapper) {
	ids := testutil.GenerateSolanaKeys(t, 4)

	_, err := m.Get(context.Background(), ids[0], solana.CommitmentMax)
	assert.Equal(t, account.ErrNotFound, err)

	assert.NoError(t, m.Add(context.Background(), ids[0], ids[1]))

	mapped, err := m.Get(context.Background(), ids[0], solana.CommitmentMax)
	assert.NoError(t, err)
	assert.EqualValues(t, mapped, ids[1])

	_, err = m.Get(context.Background(), ids[2], solana.CommitmentMax)
	assert.Equal(t, account.ErrNotFound, err)
}
