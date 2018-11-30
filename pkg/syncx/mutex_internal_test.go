package syncx

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMutex_new(t *testing.T) {
	mx := &Mutex{}

	assert.Equal(t, atomic.LoadUint32(&mx.locked), unlocked)
}

func TestMutex_Lock(t *testing.T) {
	mx := &Mutex{}

	mx.Lock()

	assert.Equal(t, atomic.LoadUint32(&mx.locked), locked)

	locked := make(chan bool)
	go func() {
		mx.Lock()
		locked <- true
	}()

	select {
	case <-locked:
		assert.Fail(t, "lock acquired", "didn't expect to be able to lock again")
	case <-time.After(100 * time.Millisecond): // timeout
	}
}

func TestMutex_Unlock(t *testing.T) {
	mx := &Mutex{locked: locked}

	mx.Unlock()

	assert.Equal(t, atomic.LoadUint32(&mx.locked), unlocked)
}

func TestMutex_TryLock(t *testing.T) {
	mx := &Mutex{}

	locked := mx.TryLock()

	assert.True(t, locked)

	locked = mx.TryLock()

	assert.False(t, locked)
}
