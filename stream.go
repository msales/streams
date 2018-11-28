package streams

// StreamBuilder represents a stream builder.
type StreamBuilder struct {
	tp *TopologyBuilder
}

// NewStreamBuilder creates a new StreamBuilder.
func NewStreamBuilder() *StreamBuilder {
	return &StreamBuilder{
		tp: NewTopologyBuilder(),
	}
}

// Source adds a Source to the stream, returning the Stream.
func (sb *StreamBuilder) Source(name string, source Source) *Stream {
	n := sb.tp.AddSource(name, source)

	return newStream(sb.tp, []Node{n})
}

// Build builds the stream Topology.
func (sb *StreamBuilder) Build() (*Topology, []error) {
	return sb.tp.Build()
}

// Stream represents a stream of data.
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

// Filter filters the stream using a Predicate.
func (s *Stream) Filter(name string, pred Predicate) *Stream {
	p := NewFilterProcessor(pred)
	n := s.tp.AddProcessor(name, p, s.parents)

	return newStream(s.tp, []Node{n})
}

// Branch branches a stream based in the given Predcates.
func (s *Stream) Branch(name string, preds ...Predicate) []*Stream {
	p := NewBranchProcessor(preds)
	n := s.tp.AddProcessor(name, p, s.parents)

	streams := make([]*Stream, 0, len(preds))
	for range preds {
		streams = append(streams, newStream(s.tp, []Node{n}))
	}
	return streams
}

// Map runs a Mapper on the stream.
func (s *Stream) Map(name string, mapper Mapper) *Stream {
	p := NewMapProcessor(mapper)
	n := s.tp.AddProcessor(name, p, s.parents)

	return newStream(s.tp, []Node{n})
}

// FlatMap runs a flat mapper on the stream.
func (s *Stream) FlatMap(name string, mapper FlatMapper) *Stream {
	p := NewFlatMapProcessor(mapper)
	n := s.tp.AddProcessor(name, p, s.parents)

	return newStream(s.tp, []Node{n})
}

// Merge merges one or more streams into this stream.
func (s *Stream) Merge(name string, streams ...*Stream) *Stream {
	parents := []Node{}
	parents = append(parents, s.parents...)
	for _, stream := range streams {
		parents = append(parents, stream.parents...)
	}

	p := NewMergeProcessor()

	n := s.tp.AddProcessor(name, p, parents)

	return newStream(s.tp, []Node{n})
}

// Print prints the data in the stream.
func (s *Stream) Print(name string) *Stream {
	return s.Process(name, NewPrintProcessor())
}

// Process runs a custom processor on the stream.
func (s *Stream) Process(name string, p Processor) *Stream {
	n := s.tp.AddProcessor(name, p, s.parents)

	return newStream(s.tp, []Node{n})
}
