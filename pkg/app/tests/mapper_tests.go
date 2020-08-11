package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kinecosystem/agora/pkg/app"
)

func RunMapperTests(t *testing.T, store app.Mapper, teardown func()) {
	for _, tf := range []func(*testing.T, app.Mapper){testMapperRoundTrip, testMapperInvalidParameters} {
		tf(t, store)
		teardown()
	}
}

func testMapperRoundTrip(t *testing.T, mapper app.Mapper) {
	t.Run("testMapperRoundTrip", func(t *testing.T) {
		appIndex, err := mapper.GetAppIndex(context.Background(), "test")
		require.Equal(t, app.ErrMappingNotFound, err)
		require.Equal(t, uint16(0), appIndex)

		err = mapper.Add(context.Background(), "test", 1)
		require.NoError(t, err)

		err = mapper.Add(context.Background(), "test", 1)
		require.Error(t, err, app.ErrExists)

		appIndex, err = mapper.GetAppIndex(context.Background(), "test")
		require.NoError(t, err)
		require.Equal(t, uint16(1), appIndex)
	})
}

func testMapperInvalidParameters(t *testing.T, mapper app.Mapper) {
	t.Run("testMapperInvalidParameters", func(t *testing.T) {
		// invalid app ID
		err := mapper.Add(context.Background(), "testtest", 1)
		require.Error(t, err)

		// invalid app index
		err = mapper.Add(context.Background(), "test", 0)
		require.Error(t, err)
	})
}
