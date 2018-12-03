package streams

import (
	"sync/atomic"
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

// Join combines contents of two Metaitems objects, updating the Metadata where necessary.
func (m Metaitems) Join(other Metaitems) Metaitems {
	OUTER:
	for _, newItem := range other {
		for _, oldItem := range m {
			if oldItem.Source == newItem.Source {
				oldItem.Metadata = oldItem.Metadata.Update(newItem.Metadata)
				continue OUTER
			}
		}

		m = append(m, newItem)
	}

	return m
}

type metastore struct {
	metadata atomic.Value // map[Processor][]Metaitem
}

// NewMetastore creates a new Metastore instance.
func NewMetastore() Metastore {
	s := &metastore{}
	s.metadata.Store(&map[Processor]Metaitems{})

	return s
}

// Pull gets and clears the processors metadata.
func (s *metastore) Pull(p Processor) (Metaitems, error) {
	meta := s.metadata.Load().(*map[Processor]Metaitems)

	items, ok := (*meta)[p]
	if ok {
		delete(*meta, p)
		return items, nil
	}

	return nil, nil
}

// PullAll gets and clears all metadata.
func (s *metastore) PullAll() (map[Processor]Metaitems, error) {
	meta := s.metadata.Load().(*map[Processor]Metaitems)

	s.metadata.Store(&map[Processor]Metaitems{})

	return *meta, nil
}

// Mark sets metadata for a processor.
func (s *metastore) Mark(p Processor, src Source, meta Metadata) error {
	if p == nil {
		return nil
	}

	procMeta := s.metadata.Load().(*map[Processor]Metaitems)

	if meta != nil {
		o := ProcessorOrigin
		if _, ok := p.(Committer); ok {
			o = CommitterOrigin
		}

		meta.WithOrigin(o)
	}

	items, ok := (*procMeta)[p]
	if !ok {
		(*procMeta)[p] = Metaitems{{Source: src, Metadata: meta}}
		return nil
	}

	if src == nil || meta == nil {
		return nil
	}

	for _, item := range items {
		if item.Source == src {
			item.Metadata = meta.Update(item.Metadata)
			return nil
		}
	}

	items = append(items, &Metaitem{Source: src, Metadata: meta})
	(*procMeta)[p] = items
	return nil
}
