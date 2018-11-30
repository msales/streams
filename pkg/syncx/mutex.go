package syncx

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	unlocked uint32 = iota
	locked

	defaultRetry = 10 * time.Millisecond
)

// Compile-time interface checks.
var (
	_ sync.Locker = (*Mutex)(nil)
	_ sync.Locker = (*NopLocker)(nil)
)

// Mutex represents a standard mutex with an added capability to immediately return if unable to acquire a lock.
type Mutex struct {
	Retry time.Duration

	locked uint32
}

// Lock locks the mutex. If already locked, it blocks.
func (m *Mutex) Lock() {
	retry := m.Retry
	if retry == 0 {
		retry = defaultRetry
	}

	for !m.TryLock() {
		time.Sleep(retry)
	}
}

// TryLock attempts to lock the mutex. It returns immediately with the result of the lock.
func (m *Mutex) TryLock() bool {
	return atomic.CompareAndSwapUint32(&m.locked, unlocked, locked)
}

// Unlock unlocks the mutex.
func (m *Mutex) Unlock() {
	atomic.StoreUint32(&m.locked, unlocked)
}

// NopLocker is a no-op implementation of Locker interface.
type NopLocker struct {}

// Lock performs no action.
func (*NopLocker) Lock() {}

// Unlock performs no action.
func (*NopLocker) Unlock() {}
