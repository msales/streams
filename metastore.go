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

// Join combines contents of 2 Metaitems objects, updating the Metadata where necessary.
func (m Metaitems) Join(other Metaitems) Metaitems {
	joined := make(Metaitems, 0, len(m))
	seen := make(map[*Metaitem]struct{}, len(m))

	for _, i1 := range m {
		joined = append(joined, i1)

		for _, i2 := range other {
			if i1.Source == i2.Source {
				i1.Metadata = i1.Metadata.Update(i2.Metadata)
				seen[i2] = struct{}{}
				break
			}
		}
	}

	for _, i := range other { // Add all unseen items from the second slice.
		if _, ok := seen[i]; !ok {
			joined = append(joined, i)
		}
	}

	return joined
}

type metastore struct {
	metadata atomic.Value // map[Processor][]Metaitem
}

// NewMetastore creates a new Metastore instance.
func NewMetastore() Metastore {
	s := &metastore{}
	s.metadata.Store(map[Processor]Metaitems{})

	return s
}

// Pull gets and clears the processors metadata.
func (s *metastore) Pull(p Processor) (Metaitems, error) {
	meta := s.metadata.Load().(map[Processor]Metaitems)

	items, ok := meta[p]
	if ok {
		delete(meta, p)
		return items, nil
	}

	return nil, nil
}

// PullAll gets and clears all metadata.
func (s *metastore) PullAll() (map[Processor]Metaitems, error) {
	meta := s.metadata.Load().(map[Processor]Metaitems)

	s.metadata.Store(map[Processor]Metaitems{})

	return meta, nil
}

// Mark sets metadata for a processor.
func (s *metastore) Mark(p Processor, src Source, meta Metadata) error {
	if p == nil || src == nil || meta == nil {
		return nil
	}

	procMeta := s.metadata.Load().(map[Processor]Metaitems)

	meta.WithOrigin(metadataOrigin(p))

	items, ok := procMeta[p]
	if !ok {
		procMeta[p] = Metaitems{{Source: src, Metadata: meta}}
		return nil
	}

	for _, item := range items {
		if item.Source == src {
			item.Metadata = meta.Update(item.Metadata)
			return nil
		}
	}

	items = append(items, &Metaitem{Source: src, Metadata: meta})
	procMeta[p] = items
	return nil
}
