package streams

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// Metastore represents a metadata store.
//
// Metastore is only partially concurrency safe. Mark and PullAll
// can be used concurrently, but Mark and Pull cannot. This is done
// to avoid locks and improve performance.
type Metastore interface {
	// Pull gets and clears the processors metadata.
	Pull(Processor) (Metaitems, error)
	// PullAll gets and clears all metadata.
	PullAll() (map[Processor]Metaitems, error)
	// Mark sets metadata for a processor.
	Mark(Processor, Source, Metadata) error
}

// Metaitem represents the source metadata combination.
type Metaitem struct {
	Source   Source
	Metadata Metadata
}

// Metaitems represents a slice of Metaitem pointers.
type Metaitems []*Metaitem

// Merge combines contents of two Metaitems objects, merging the Metadata where necessary.
func (m Metaitems) Merge(items Metaitems, strategy MetadataStrategy) Metaitems {
OUTER:
	for _, newItem := range items {
		for _, oldItem := range m {
			if oldItem.Source == newItem.Source {
				if oldItem.Metadata != nil {
					oldItem.Metadata = oldItem.Metadata.Merge(newItem.Metadata, strategy)
				}
				continue OUTER
			}
		}

		m = append(m, newItem)
	}

	return m
}

type metastore struct {
	metadata *atomic.Value // map[Processor][]Metaitem

	procMu sync.Mutex
}

// NewMetastore creates a new Metastore instance.
func NewMetastore() Metastore {
	s := &metastore{
		metadata: &atomic.Value{},
	}
	s.metadata.Store(&map[Processor]Metaitems{})

	return s
}

// Pull gets and clears the processors metadata.
func (s *metastore) Pull(p Processor) (Metaitems, error) {
	s.procMu.Lock()

	meta := s.metadata.Load().(*map[Processor]Metaitems)

	// We dont lock here as the Pump should be locked at this point
	items, ok := (*meta)[p]
	if ok {
		delete(*meta, p)
		s.procMu.Unlock()

		return items, nil
	}

	s.procMu.Unlock()

	return nil, nil
}

// PullAll gets and clears all metadata.
func (s *metastore) PullAll() (map[Processor]Metaitems, error) {
	oldMeta := s.pullMetadata()

	// Make sure no marks are happening on the old metadata
	s.procMu.Lock()
	s.procMu.Unlock() //lint:ignore SA2001 syncpoint

	return oldMeta, nil
}

// pullMetadata atomically replaces the current metadata with a new instance and returns the old instance.
func (s *metastore) pullMetadata() map[Processor]Metaitems {
	newMeta := atomic.Value{}
	newMeta.Store(&map[Processor]Metaitems{})

	metaPtr := (*unsafe.Pointer)(unsafe.Pointer(&s.metadata))
	oldMetaPtr := atomic.SwapPointer(metaPtr, unsafe.Pointer(&newMeta))

	return *(*atomic.Value)(oldMetaPtr).Load().(*map[Processor]Metaitems)
}

// Mark sets metadata for a processor.
func (s *metastore) Mark(p Processor, src Source, meta Metadata) error {
	if p == nil {
		return nil
	}

	if meta != nil {
		o := ProcessorOrigin
		if _, ok := p.(Committer); ok {
			o = CommitterOrigin
		}

		meta.WithOrigin(o)
	}

	s.procMu.Lock()

	procMeta := s.metadata.Load().(*map[Processor]Metaitems)

	items, ok := (*procMeta)[p]
	if !ok {
		(*procMeta)[p] = Metaitems{{Source: src, Metadata: meta}}

		s.procMu.Unlock()
		return nil
	}

	if src == nil || meta == nil {
		s.procMu.Unlock()
		return nil
	}

	for _, item := range items {
		if item.Source == src {
			item.Metadata = meta.Merge(item.Metadata, Dupless)

			s.procMu.Unlock()
			return nil
		}
	}

	items = append(items, &Metaitem{Source: src, Metadata: meta})
	(*procMeta)[p] = items

	s.procMu.Unlock()
	return nil
}
