package tests

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/kinecosystem/agora/pkg/transaction/history/ingestion"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type LockCtor func(key string) (ingestion.DistributedLock, error)

func RunLockTests(t *testing.T, lockCtor LockCtor, teardown func()) {
	for _, tf := range []func(*testing.T, LockCtor){testLocker_ExclusiveAccess, testLocker_MultipleLocks} {
		tf(t, lockCtor)
		teardown()
	}
}

func testLocker_ExclusiveAccess(t *testing.T, lockCtor LockCtor) {
	t.Run("TestLocker_ExclusiveAccess", func(t *testing.T) {
		var values []int

		var wg sync.WaitGroup
		writer := func(id int) {
			l, err := lockCtor("master")
			require.NoError(t, err)

			require.NoError(t, l.Lock(context.Background()))
			for i := 0; i < 10; i++ {
				values = append(values, id)

				// We sleep a little bit in order to encourage striped
				// writes (which should be guarded against by
				// the locker). While it doesn't guarentee lock safety,
				// per se, it helps increase the likely hood of failing
				// the test if the lock isn't working correctly.
				time.Sleep(10 * time.Duration(id) * time.Millisecond)
			}
			require.NoError(t, l.Unlock())
			wg.Done()
		}

		wg.Add(2)
		go writer(1)
		go writer(2)
		wg.Wait()

		assert.Len(t, values, 20)
		for i := 1; i < 10; i++ {
			assert.Equal(t, values[i-1], values[i])
		}
		for i := 11; i < 20; i++ {
			assert.Equal(t, values[i-1], values[i])
		}
	})
}

func testLocker_MultipleLocks(t *testing.T, lockCtor LockCtor) {
	t.Run("TestLocker_MultipleLocks", func(t *testing.T) {
		var mu sync.Mutex
		var values []int

		var wg sync.WaitGroup
		writer := func(id int) {
			l, err := lockCtor(strconv.Itoa(id))
			require.NoError(t, err)

			require.NoError(t, l.Lock(context.Background()))
			for i := 0; i < 10; i++ {
				mu.Lock()
				values = append(values, id)
				mu.Unlock()

				time.Sleep(10 * time.Duration(id) * time.Millisecond)
			}
			require.NoError(t, l.Unlock())
			wg.Done()
		}

		wg.Add(2)
		go writer(1)
		go writer(2)
		wg.Wait()

		assert.Len(t, values, 20)

		diffCount := 0
		for i := 1; i < len(values); i++ {
			if values[i-1] != values[i] {
				diffCount++
			}
		}

		assert.True(t, diffCount > 1)
	})
}
