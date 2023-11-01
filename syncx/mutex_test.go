package syncx_test

import (
	"github.com/msales/streams/v7/syncx"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMutexLayout(t *testing.T) {
	sf := reflect.TypeOf((*syncx.Mutex)(nil)).Elem().FieldByIndex([]int{0, 0})

	if sf.Name != "state" {
		assert.FailNow(t, "sync.Mutex first field should have name state")
	}

	if sf.Offset != uintptr(0) {
		assert.FailNow(t, "sync.Mutex state field should have zero offset")
	}

	if sf.Type != reflect.TypeOf(int32(1)) {
		assert.FailNow(t, "sync.Mutex state field type should be int32")
	}
}

func TestMutex_ImplementsLocker(t *testing.T) {
	var mu syncx.Mutex

	assert.Implements(t, (*sync.Locker)(nil), &mu)
}

func TestMutex_TryLock(t *testing.T) {
	var mu syncx.Mutex
	if !mu.TryLock() {
		assert.FailNow(t, "mutex must be unlocked")
	}
	if mu.TryLock() {
		assert.FailNow(t, "mutex must be locked")
	}

	mu.Unlock()
	if !mu.TryLock() {
		assert.FailNow(t, "mutex must be unlocked")
	}
	if mu.TryLock() {
		assert.FailNow(t, "mutex must be locked")
	}

	mu.Unlock()
	mu.Lock()
	if mu.TryLock() {
		assert.FailNow(t, "mutex must be locked")
	}
	if mu.TryLock() {
		assert.FailNow(t, "mutex must be locked")
	}
	mu.Unlock()
}

func TestMutex_TryLockPointer(t *testing.T) {
	mu := &syncx.Mutex{}
	if !mu.TryLock() {
		assert.FailNow(t, "mutex must be unlocked")
	}
	if mu.TryLock() {
		assert.FailNow(t, "mutex must be locked")
	}

	mu.Unlock()
	if !mu.TryLock() {
		assert.FailNow(t, "mutex must be unlocked")
	}
	if mu.TryLock() {
		assert.FailNow(t, "mutex must be locked")
	}

	mu.Unlock()
	mu.Lock()
	if mu.TryLock() {
		assert.FailNow(t, "mutex must be locked")
	}
	if mu.TryLock() {
		assert.FailNow(t, "mutex must be locked")
	}
	mu.Unlock()
}

func TestMutex_Race(t *testing.T) {
	var mu syncx.Mutex
	var x int
	for i := 0; i < 1024; i++ {
		if i%2 == 0 {
			go func() {
				if mu.TryLock() {
					x++
					mu.Unlock()
				}
			}()
			continue
		}

		go func() {
			mu.Lock()
			x++
			mu.Unlock()
		}()
	}
}
