package streams

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/msales/pkg/v3/stats"
	"github.com/msales/pkg/v3/syncx"
)

const (
	stopped uint32 = iota
	running
)

var (
	// ErrNotRunning is returned when trying to perform an action that requires a running supervisor.
	ErrNotRunning = errors.New("streams: supervisor not running")
	// ErrAlreadyRunning is returned when starting a supervisor that has already been started.
	ErrAlreadyRunning = errors.New("streams: supervisor already running")
	// ErrUnknownPump is returned when the supervisor is unable to find a pump for a given processor.
	ErrUnknownPump = errors.New("streams: encountered an unknown pump")
)

// NopLocker is a no-op implementation of Locker interface.
type nopLocker struct{}

// Lock performs no action.
func (*nopLocker) Lock() {}

// Unlock performs no action.
func (*nopLocker) Unlock() {}

// Supervisor represents a concurrency-safe stream supervisor.
//
// The Supervisor performs a commit in a concurrently-safe manner.
// There can only ever be 1 ongoing commit at any given time.
type Supervisor interface {
	io.Closer

	// WithContext sets the context.
	WithContext(context.Context)

	// WithPumps sets a map of Pumps.
	WithPumps(map[Node]Pump)

	// Start starts the supervisor.
	//
	// This function should initiate all the background tasks of the Supervisor.
	// It must not be a blocking call.
	Start() error

	// Commit performs a global commit sequence.
	//
	// If triggered by a Pipe, the associated Processor should be passed.
	Commit(Processor) error
}

type supervisor struct {
	store    Metastore
	strategy MetadataStrategy

	ctx   context.Context
	stats stats.Stats

	pumps map[Processor]Pump

	commitMu syncx.Mutex
}

// NewSupervisor returns a new Supervisor instance.
func NewSupervisor(store Metastore, strategy MetadataStrategy) Supervisor {
	return &supervisor{
		store:    store,
		strategy: strategy,
		ctx:      context.Background(),
		stats:    stats.Null,
	}
}

// Start starts the supervisor.
//
// This function should initiate all the background tasks of the Supervisor.
// It must not be a blocking call.
func (s *supervisor) Start() error {
	return nil
}

// WithContext sets the context.
func (s *supervisor) WithContext(ctx context.Context) {
	s.ctx = ctx

	if st, ok := stats.FromContext(ctx); ok {
		s.stats = st
	}
}

// WithPumps sets a map of Pumps.
func (s *supervisor) WithPumps(pumps map[Node]Pump) {
	mapped := make(map[Processor]Pump, len(pumps))
	for node, pump := range pumps {
		mapped[node.Processor()] = pump
	}

	s.pumps = mapped
}

// Commit performs a global commit sequence.
//
// If triggered by a Pipe, the associated Processor should be passed.
func (s *supervisor) Commit(caller Processor) error {
	if !s.commitMu.TryLock() {
		// We are already committing.
		return nil
	}
	defer s.commitMu.Unlock()

	start := nanotime()

	metadata, err := s.store.PullAll()
	if err != nil {
		return err
	}

	var metaItems Metaitems
	for proc, items := range metadata {
		if comm, ok := proc.(Committer); ok {
			newItems, err := s.commit(caller, comm)
			if err != nil {
				return err
			}

			items = items.Merge(newItems, Dupless)
		}

		metaItems = metaItems.Merge(items, s.strategy)
	}

	for _, item := range metaItems {
		if item.Source == nil {
			continue
		}

		err := item.Source.Commit(item.Metadata)
		if err != nil {
			return err
		}
	}

	latency := time.Duration(nanotime() - start)
	_ = s.stats.Timing("commit.latency", latency, 1)
	_ = s.stats.Inc("commit.commits", 1, 1)

	return nil
}

func (s *supervisor) commit(caller Processor, comm Committer) (Metaitems, error) {
	locker, err := s.getLocker(caller, comm)
	if err != nil {
		return nil, err
	}

	locker.Lock()
	defer locker.Unlock()

	err = comm.Commit(s.ctx)
	if err != nil {
		return nil, err
	}

	// Pull metadata of messages that have been processed between the initial pull and the lock.
	return s.store.Pull(comm)
}

func (s *supervisor) getLocker(caller, proc Processor) (sync.Locker, error) {
	if caller == proc {
		return &nopLocker{}, nil
	}

	pump, ok := s.pumps[proc]
	if !ok {
		return nil, ErrUnknownPump
	}

	return pump, nil
}

// Permanently locks the supervisor, ensuring that no commit will ever be executed.
func (s *supervisor) Close() error {
	s.commitMu.Lock()

	return nil
}

type timedSupervisor struct {
	ctx context.Context

	inner Supervisor
	d     time.Duration
	errFn ErrorFunc

	t       *time.Ticker
	commits uint32
	running uint32
}

// NewTimedSupervisor returns a supervisor that commits automatically.
func NewTimedSupervisor(inner Supervisor, d time.Duration, errFn ErrorFunc) Supervisor {
	return &timedSupervisor{
		inner: inner,
		d:     d,
		errFn: errFn,
	}
}

// WithContext sets the context.
func (s *timedSupervisor) WithContext(ctx context.Context) {
	s.inner.WithContext(ctx)
}

// WithPumps sets a map of Pumps.
func (s *timedSupervisor) WithPumps(pumps map[Node]Pump) {
	s.inner.WithPumps(pumps)
}

// Start starts the supervisor.
//
// This function should initiate all the background tasks of the Supervisor.
// It must not be a blocking call.
func (s *timedSupervisor) Start() error {
	if !s.setRunning() {
		return ErrAlreadyRunning
	}

	s.t = time.NewTicker(s.d)

	go func() {
		for range s.t.C {
			// If there was a commit triggered "manually" by a Committer, skip a single timed commit.
			if atomic.LoadUint32(&s.commits) > 0 {
				atomic.StoreUint32(&s.commits, 0)
				continue
			}

			err := s.inner.Commit(nil)
			if err != nil {
				s.errFn(err)
			}
		}
	}()

	return s.inner.Start()
}

// Close stops the timer and closes the inner supervisor.
func (s *timedSupervisor) Close() error {
	if !s.setStopped() {
		return ErrNotRunning
	}

	s.t.Stop()

	return s.inner.Close()
}

// Commit performs a global commit sequence.
//
// If triggered by a Pipe, the associated Processor should be passed.
func (s *timedSupervisor) Commit(caller Processor) error {
	if !s.isRunning() {
		return ErrNotRunning
	}

	// Increment the commit count
	atomic.AddUint32(&s.commits, 1)

	err := s.inner.Commit(caller)
	if err != nil {
		return err
	}

	return nil
}

func (s *timedSupervisor) setRunning() bool {
	return atomic.CompareAndSwapUint32(&s.running, stopped, running)
}

func (s *timedSupervisor) isRunning() bool {
	return running == atomic.LoadUint32(&s.running)
}

func (s *timedSupervisor) setStopped() bool {
	return atomic.CompareAndSwapUint32(&s.running, running, stopped)
}
