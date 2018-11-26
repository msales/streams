package streams

type Metastore interface {
	Pull(Processor) (map[Source]Metadata, error)
	PullAll() (map[Processor]map[Source]Metadata, error)
	Mark(Processor, Source, Metadata) error
}

type metastore struct {

}

func NewMetastore() Metastore {
	return &metastore{}
}

func (s *metastore) Pull(p Processor) (map[Source]Metadata, error) {
	panic("TODO")
}

func (s *metastore) PullAll() (map[Processor]map[Source]Metadata, error) {
	panic("TODO")
}

func (s *metastore) Mark(p Processor, src Source, meta Metadata) error {
	panic("TODO")
}


