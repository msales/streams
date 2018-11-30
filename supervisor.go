package streams

import (
	"errors"
	"io"
	"sync/atomic"
	"time"
)

const (
	unlocked uint32 = iota
	locked

	closeRetry = 10 * time.Millisecond
)

var ErrUnknownPump = errors.New("supervisor: encountered an unknown pump")

// Supervisor represents a concurrency-safe stream supervisor.
//
// The Supervisor performs a commit in a concurrently-safe manner.
// There can only ever be 1 ongoing commit at any given time.
type Supervisor interface {
	io.Closer

	// Perform a global commit sequence.
	//
	// If triggered by a Pipe, the associated Processor should be passed.
	Commit(Processor) error
	// WithPumps sets map of Pumps.
	WithPumps(pumps map[Node]Pump)
}

type supervisor struct {
	store Metastore

	pumps map[Processor]Pump

	locked uint32
}

// NewSupervisor returns a new Supervisor instance.
func NewSupervisor(store Metastore) Supervisor {
	return &supervisor{
		store: store,
	}
}

// Permanently locks the supervisor, ensuring that no commit will be executed.
func (s *supervisor) Close() error {
	for !s.tryLock() {
		time.Sleep(closeRetry)
	}

	return nil
}

// WithPumps sets map of Pumps.
func (s *supervisor) WithPumps(pumps map[Node]Pump) {
	s.pumps = remapPumps(pumps)
}

// Perform a global commit sequence.
//
// If triggered by a Pipe, the associated Processor should be passed.
func (s *supervisor) Commit(p Processor) error {
	if ok := s.tryLock(); !ok {
		return nil
	}
	defer s.unlock()

	metadata, err := s.store.PullAll()
	if err != nil {
		return err
	}

	srcMeta := make(sourceMetadata)

	for proc, items := range metadata {
		items, err := s.commit(proc, items)
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

// tryLock tries to lock the supervisor for the commit and returns the result.
// Must be called before any commit-related actions.
func (s *supervisor) tryLock() bool {
	return atomic.CompareAndSwapUint32(&s.locked, unlocked, locked)
}

// unlock unlocks the supervisor.
// Must be called after the commit is done, whatever the result.
func (s *supervisor) unlock() {
	atomic.StoreUint32(&s.locked, unlocked)
}

func (s *supervisor) commit(proc Processor, items []*Metaitem) ([]*Metaitem, error) {
	if cmt, ok := proc.(Committer); ok {
		pump, ok := s.pumps[proc]
		if !ok {
			return nil, ErrUnknownPump
		}

		err := pump.WithLock(func() error {
			err := cmt.Commit()
			if err != nil {
				return err
			}

			// Pull metadata of messages that have been processed between the initial pull and the lock
			newItems, err := s.store.Pull(proc)
			if err != nil {
				return err
			}

			items = append(items, newItems...)

			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	return items, nil
}

// sourceMetadata maps Metadata to each known Source.
type sourceMetadata map[Source]Metadata

// Merge merges metadata from metaitems.
func (m sourceMetadata) Merge(items []*Metaitem) sourceMetadata {
	for _, item := range items {
		src, ok := m[item.Source]
		if ok {
			m[item.Source] = src.Merge(item.Metadata)
		} else {
			m[item.Source] = item.Metadata
		}
	}
	return m
}

func remapPumps(pumps map[Node]Pump) map[Processor]Pump {
	mapped := make(map[Processor]Pump, len(pumps))
	for node, pump := range pumps {
		mapped[node.Processor()] = pump
	}
	return mapped
}
