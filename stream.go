package streams

type StreamBuilder struct {
	tp *TopologyBuilder
}

func NewStreamBuilder() *StreamBuilder {
	return &StreamBuilder{
		tp: NewTopologyBuilder(),
	}
}

func (sb *StreamBuilder) Source(name string, source Source) *Stream {
	n := sb.tp.AddSource(name, source)

	return newStream(sb.tp, []Node{n})
}

func (sb *StreamBuilder) Build() *Topology {
	return sb.tp.Build()
}

type Stream struct {
	tp      *TopologyBuilder
	parents []Node
}

func newStream(tp *TopologyBuilder, parents []Node) *Stream {
	return &Stream{
		tp:      tp,
		parents: parents,
	}
}

func (s *Stream) Filter(name string, pred Predicate) *Stream {
	p := NewFilterProcessor(pred)
	n := s.tp.AddProcessor(name, p, s.parents)

	return newStream(s.tp, []Node{n})
}

func (s *Stream) Branch(name string, preds ...Predicate) []*Stream {
	p := NewBranchProcessor(preds)
	n := s.tp.AddProcessor(name, p, s.parents)

	streams := make([]*Stream, 0, len(preds))
	for range preds {
		streams = append(streams, newStream(s.tp, []Node{n}))
	}
	return streams
}

func (s *Stream) Map(name string, mapper Mapper) *Stream {
	p := NewMapProcessor(mapper)
	n := s.tp.AddProcessor(name, p, s.parents)

	return newStream(s.tp, []Node{n})
}

func (s *Stream) FlatMap(name string, mapper FlatMapper) *Stream {
	p := NewFlatMapProcessor(mapper)
	n := s.tp.AddProcessor(name, p, s.parents)

	return newStream(s.tp, []Node{n})
}

func (s *Stream) Merge(name string, streams ...*Stream) *Stream {
	parents := []Node{}
	parents = append(parents, s.parents...)
	for _, stream := range streams {
		parents = append(parents, stream.parents...)
	}

	p := NewPassThroughProcessor()

	n := s.tp.AddProcessor(name, p, parents)

	return newStream(s.tp, []Node{n})
}

func (s *Stream) Print(name string) *Stream {
	return s.Process(name, NewPrintProcessor())
}

func (s *Stream) Process(name string, p Processor) *Stream {
	n := s.tp.AddProcessor(name, p, s.parents)

	return newStream(s.tp, []Node{n})
}
