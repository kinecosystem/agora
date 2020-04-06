package static

import (
	"context"
	"testing"

	"github.com/kinecosystem/agora-common/kin"
	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora-transaction-services-internal/pkg/appindex"
)

func TestResolver(t *testing.T) {
	r := New()

	m, err := kin.NewMemo(1, kin.TransactionTypeEarn, 0, make([]byte, 29))
	require.NoError(t, err)

	// todo: verify result testing against some kind of comformant spec
	url, err := r.Resolve(context.Background(), m)
	require.NoError(t, err)
	require.NotEmpty(t, url)
	require.NoError(t, url.Validate())

	m, err = kin.NewMemo(1, kin.TransactionTypeEarn, 20, make([]byte, 29))
	require.NoError(t, err)

	url, err = r.Resolve(context.Background(), m)
	require.Equal(t, appindex.ErrNotFound, err)
	require.Empty(t, url)
}
