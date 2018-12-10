package syncx_test

import (
	"reflect"
	"testing"

	"github.com/msales/streams/v2/pkg/syncx"
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

func TestTryLock(t *testing.T) {
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

func TestTryLockPointer(t *testing.T) {
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

func TestRace(t *testing.T) {
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

func TestNopLocker_Lock(t *testing.T) {
	var mu syncx.NopLocker

	mu.Lock()
}

func TestNopLocker_UnLock(t *testing.T) {
	var mu syncx.NopLocker

	mu.Unlock()
}
