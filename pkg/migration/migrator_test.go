package migration

import (
	"context"
	"testing"

	"github.com/kinecosystem/agora-common/headers"
	"github.com/kinecosystem/agora-common/solana"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/testutil"
	"github.com/kinecosystem/agora/pkg/version"
)

func TestContextAwareMigrator(t *testing.T) {
	base := &mockMigrator{}
	account := testutil.GenerateSolanaKeys(t, 1)[0]

	ctx, err := headers.ContextWithHeaders(context.Background())
	require.NoError(t, err)

	m := NewContextAwareMigrator(base)

	assert.Nil(t, m.InitiateMigration(ctx, account, false, solana.CommitmentRecent))
	assert.Empty(t, base.Calls)

	require.NoError(t, headers.SetASCIIHeader(ctx, version.DesiredKinVersionHeader, "4"))

	base.On("InitiateMigration", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	assert.Nil(t, m.InitiateMigration(ctx, account, false, solana.CommitmentRecent))
	assert.Len(t, base.Calls, 1)
}
