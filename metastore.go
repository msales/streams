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
	Pull(Processor) ([]Metaitem, error)
	// PullAll gets and clears all metadata.
	PullAll() (map[Processor][]Metaitem, error)
	// Mark sets metadata for a processor.
	Mark(Processor, Source, Metadata) error
}

type Metaitem struct {
	Source   Source
	Metadata Metadata
}

type metastore struct {
	metadata atomic.Value // map[Processor]map[Source]Metadata
}

// NewMetastore creates a new Metastore instance.
func NewMetastore() Metastore {
	s := &metastore{}
	s.metadata.Store(map[Processor][]Metaitem{})

	return s
}

// Pull gets and clears the processors metadata.
func (s *metastore) Pull(p Processor) ([]Metaitem, error) {
	meta := s.metadata.Load().(map[Processor][]Metaitem)

	items, ok := meta[p]
	if ok {
		delete(meta, p)
		return items, nil
	}

	return nil, nil
}

// PullAll gets and clears all metadata.
func (s *metastore) PullAll() (map[Processor][]Metaitem, error) {
	meta := s.metadata.Load().(map[Processor][]Metaitem)

	s.metadata.Store(map[Processor][]Metaitem{})

	return meta, nil
}

// Mark sets metadata for a processor.
func (s *metastore) Mark(p Processor, src Source, meta Metadata) error {
	procMeta := s.metadata.Load().(map[Processor][]Metaitem)

	items, ok := procMeta[p]
	if !ok {
		procMeta[p] = []Metaitem{{Source: src, Metadata: meta}}
		return nil
	}

	for _, item := range items {
		if item.Source == src {
			item.Metadata = meta.Merge(item.Metadata)
			return nil
		}
	}

	items = append(items, Metaitem{Source: src, Metadata: meta})
	procMeta[p] = items
	return nil
}