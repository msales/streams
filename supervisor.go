package streams

import (
	"errors"
	"io"
	"sync"

	"github.com/msales/streams/pkg/syncx"
)

// ErrUnknownPump is returned when the supervisor is unable to find a pump for a given processor.
var ErrUnknownPump = errors.New("streams: encountered an unknown pump")

// Supervisor represents a concurrency-safe stream supervisor.
//
// The Supervisor performs a commit in a concurrently-safe manner.
// There can only ever be 1 ongoing commit at any given time.
type Supervisor interface {
	io.Closer

	// Commit performs a global commit sequence.
	//
	// If triggered by a Pipe, the associated Processor should be passed.
	Commit(Processor) error
	// WithPumps sets map of Pumps.
	WithPumps(pumps map[Node]Pump)
}

type supervisor struct {
	store Metastore

	pumps map[Processor]Pump

	mx syncx.Mutex
}

// NewSupervisor returns a new Supervisor instance.
func NewSupervisor(store Metastore) Supervisor {
	return &supervisor{
		store: store,
	}
}

// Permanently locks the supervisor, ensuring that no commit will ever be executed.
func (s *supervisor) Close() error {
	s.mx.Lock()

	return nil
}

// WithPumps sets map of Pumps.
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
	if ok := s.mx.TryLock(); !ok {
		return nil
	}
	defer s.mx.Unlock()

	metadata, err := s.store.PullAll()
	if err != nil {
		return err
	}

	srcMeta := make(sourceMetadata)

	for proc, items := range metadata {
		items, err := s.commit(caller, proc, items)
		if err != nil {
			return err
		}

		srcMeta.Merge(items)
	}

	for src, meta := range srcMeta {
		err := src.Commit(meta)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *supervisor) commit(caller, proc Processor, items Metaitems) (Metaitems, error) {
	if cmt, ok := proc.(Committer); ok {
		locker, err := s.getLocker(caller, proc)
		if err != nil {
			return nil, err
		}

		locker.Lock()
		defer locker.Unlock()

		err = cmt.Commit()
		if err != nil {
			return nil, err
		}

		// Pull metadata of messages that have been processed between the initial pull and the lock
		newItems, err := s.store.Pull(proc)
		if err != nil {
			return nil, err
		}

		items = items.Join(newItems)
	}

	return items, nil
}

func (s *supervisor) getLocker(caller, proc Processor) (sync.Locker, error) {
	if caller == proc {
		return &syncx.NopLocker{}, nil
	}

	pump, ok := s.pumps[proc]
	if !ok {
		return nil, ErrUnknownPump
	}

	return pump, nil
}

// sourceMetadata maps Metadata to each known Source.
type sourceMetadata map[Source]Metadata

// Merge merges metadata from metaitems.
func (m sourceMetadata) Merge(items Metaitems) {
	for _, item := range items {
		m[item.Source] = item.Metadata.Merge(m[item.Source])
	}
}
