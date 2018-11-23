package streams

// Mergeable represents metadata that can be merged.
type Mergeable interface {
	// Merge merges two pieces of metadata.
	Merge(interface{}) interface{}
}

type Metastore interface {
	Mark(n Node, src Source, meta interface{}) error
	Commit(n Node, src Source, meta interface{}) error
}

type metastore struct {

}

func NewMetastore() Metastore {
	return &metastore{}
}

func (s *metastore) Mark(n Node, src Source, meta interface{}) error {
	panic("TODO")
}

func (s *metastore) Commit(n Node, src Source, meta interface{}) error {
	panic("TODO")
}


