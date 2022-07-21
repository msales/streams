package streams

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStreamBuilder_Source(t *testing.T) {
	source := &streamSource{}
	builder := NewStreamBuilder()

	stream := builder.Source("test", source)

	assert.Len(t, stream.parents, 1)
	assert.IsType(t, &SourceNode{}, stream.parents[0])
	assert.IsType(t, stream.parents[0].(*SourceNode).Name(), "test")
}

func TestStreamBuilder_Build(t *testing.T) {
	proc := &streamProcessor{}
	source := &streamSource{}
	builder := NewStreamBuilder()
	builder.Source("test", source).Process("test1", proc)

	top, errs := builder.Build()

	assert.Len(t, errs, 0)
	assert.Len(t, top.Sources(), 1)
	assert.Len(t, top.Processors(), 1)
	assert.Contains(t, top.Sources(), source)
}

func TestStream_Filter(t *testing.T) {
	source := &streamSource{}
	builder := NewStreamBuilder()

	stream := builder.Source("source", source).
		Filter("test", PredicateFunc(func(msg Message) (bool, error) {
			return true, nil
		}))

	assert.Len(t, stream.parents, 1)
	assert.IsType(t, &ProcessorNode{}, stream.parents[0])
	assert.Equal(t, stream.parents[0].(*ProcessorNode).name, "test")
	assert.IsType(t, &FilterProcessor{}, stream.parents[0].(*ProcessorNode).processor)
}

func TestStream_FilterFunc(t *testing.T) {
	source := &streamSource{}
	builder := NewStreamBuilder()

	stream := builder.Source("source", source).
		FilterFunc("test", func(msg Message) (bool, error) {
			return true, nil
		})

	assert.Len(t, stream.parents, 1)
	assert.IsType(t, &ProcessorNode{}, stream.parents[0])
	assert.Equal(t, stream.parents[0].(*ProcessorNode).name, "test")
	assert.IsType(t, &FilterProcessor{}, stream.parents[0].(*ProcessorNode).processor)
}

func TestStream_Branch(t *testing.T) {
	source := &streamSource{}
	builder := NewStreamBuilder()

	streams := builder.Source("source", source).Branch(
		"test",
		PredicateFunc(func(msg Message) (bool, error) {
			return true, nil
		}),
		PredicateFunc(func(msg Message) (bool, error) {
			return true, nil
		}),
	)

	assert.Len(t, streams, 2)
	assert.Len(t, streams[0].parents, 1)
	assert.IsType(t, &ProcessorNode{}, streams[0].parents[0])
	assert.Equal(t, streams[0].parents[0].(*ProcessorNode).name, "test")
	assert.IsType(t, &BranchProcessor{}, streams[0].parents[0].(*ProcessorNode).processor)
	assert.Len(t, streams[1].parents, 1)
	assert.IsType(t, &ProcessorNode{}, streams[1].parents[0])
	assert.Equal(t, streams[1].parents[0].(*ProcessorNode).name, "test")
	assert.IsType(t, &BranchProcessor{}, streams[1].parents[0].(*ProcessorNode).processor)
}

func TestStream_BranchFunc(t *testing.T) {
	source := &streamSource{}
	builder := NewStreamBuilder()

	streams := builder.Source("source", source).BranchFunc(
		"test",
		func(msg Message) (bool, error) {
			return true, nil
		},
		func(msg Message) (bool, error) {
			return true, nil
		},
	)

	assert.Len(t, streams, 2)
	assert.Len(t, streams[0].parents, 1)
	assert.IsType(t, &ProcessorNode{}, streams[0].parents[0])
	assert.Equal(t, streams[0].parents[0].(*ProcessorNode).name, "test")
	assert.IsType(t, &BranchProcessor{}, streams[0].parents[0].(*ProcessorNode).processor)
	assert.Len(t, streams[1].parents, 1)
	assert.IsType(t, &ProcessorNode{}, streams[1].parents[0])
	assert.Equal(t, streams[1].parents[0].(*ProcessorNode).name, "test")
	assert.IsType(t, &BranchProcessor{}, streams[1].parents[0].(*ProcessorNode).processor)
}

func TestStream_Map(t *testing.T) {
	source := &streamSource{}
	builder := NewStreamBuilder()

	stream := builder.Source("source", source).
		Map("test", MapperFunc(func(msg Message) (Message, error) {
			return EmptyMessage, nil
		}))

	assert.Len(t, stream.parents, 1)
	assert.IsType(t, &ProcessorNode{}, stream.parents[0])
	assert.Equal(t, stream.parents[0].(*ProcessorNode).name, "test")
	assert.IsType(t, &MapProcessor{}, stream.parents[0].(*ProcessorNode).processor)
}

func TestStream_MapFunc(t *testing.T) {
	source := &streamSource{}
	builder := NewStreamBuilder()

	stream := builder.Source("source", source).
		MapFunc("test", func(msg Message) (Message, error) {
			return EmptyMessage, nil
		})

	assert.Len(t, stream.parents, 1)
	assert.IsType(t, &ProcessorNode{}, stream.parents[0])
	assert.Equal(t, stream.parents[0].(*ProcessorNode).name, "test")
	assert.IsType(t, &MapProcessor{}, stream.parents[0].(*ProcessorNode).processor)
}

func TestStream_FlatMap(t *testing.T) {
	source := &streamSource{}
	builder := NewStreamBuilder()

	stream := builder.Source("source", source).
		FlatMap("test", FlatMapperFunc(func(msg Message) ([]Message, error) {
			return nil, nil
		}))

	assert.Len(t, stream.parents, 1)
	assert.IsType(t, &ProcessorNode{}, stream.parents[0])
	assert.Equal(t, stream.parents[0].(*ProcessorNode).name, "test")
	assert.IsType(t, &FlatMapProcessor{}, stream.parents[0].(*ProcessorNode).processor)
}

func TestStream_FlatMapFunc(t *testing.T) {
	source := &streamSource{}
	builder := NewStreamBuilder()

	stream := builder.Source("source", source).
		FlatMapFunc("test", func(msg Message) ([]Message, error) {
			return nil, nil
		})

	assert.Len(t, stream.parents, 1)
	assert.IsType(t, &ProcessorNode{}, stream.parents[0])
	assert.Equal(t, stream.parents[0].(*ProcessorNode).name, "test")
	assert.IsType(t, &FlatMapProcessor{}, stream.parents[0].(*ProcessorNode).processor)
}

func TestStream_Merge(t *testing.T) {
	source := &streamSource{}
	builder := NewStreamBuilder()

	stream1 := builder.Source("source1", source)
	stream2 := builder.Source("source2", source)

	stream := stream2.Merge("test", stream1)

	assert.Len(t, stream.parents, 1)
	assert.IsType(t, &ProcessorNode{}, stream.parents[0])
	assert.Equal(t, stream.parents[0].(*ProcessorNode).name, "test")
	assert.IsType(t, &MergeProcessor{}, stream.parents[0].(*ProcessorNode).processor)
}

func TestStream_Print(t *testing.T) {
	source := &streamSource{}
	builder := NewStreamBuilder()

	stream := builder.Source("source", source).Print("test")

	assert.Len(t, stream.parents, 1)
	assert.IsType(t, &ProcessorNode{}, stream.parents[0])
	assert.Equal(t, stream.parents[0].(*ProcessorNode).name, "test")
	assert.IsType(t, &PrintProcessor{}, stream.parents[0].(*ProcessorNode).processor)
}

func TestStream_Process(t *testing.T) {
	proc := &streamProcessor{}
	source := &streamSource{}
	builder := NewStreamBuilder()

	stream := builder.Source("source", source).Process("test", proc)

	assert.Len(t, stream.parents, 1)
	assert.IsType(t, &ProcessorNode{}, stream.parents[0])
	assert.Equal(t, stream.parents[0].(*ProcessorNode).name, "test")
	assert.Equal(t, stream.parents[0].(*ProcessorNode).processor, proc)
}

func TestStream_FanOut(t *testing.T) {
	source := &streamSource{}
	builder := NewStreamBuilder()

	streams := builder.Source("source", source).FanOut("test", 2)

	assert.Len(t, streams, 2)
	assert.Len(t, streams[0].parents, 1)
	assert.IsType(t, &ProcessorNode{}, streams[0].parents[0])
	assert.Equal(t, streams[0].parents[0].(*ProcessorNode).name, "test")
	assert.IsType(t, &FanOutProcessor{}, streams[0].parents[0].(*ProcessorNode).processor)
	assert.Len(t, streams[1].parents, 1)
	assert.IsType(t, &ProcessorNode{}, streams[1].parents[0])
	assert.Equal(t, streams[1].parents[0].(*ProcessorNode).name, "test")
	assert.IsType(t, &FanOutProcessor{}, streams[1].parents[0].(*ProcessorNode).processor)
}

type streamSource struct{}

func (s streamSource) Consume() (Message, error) {
	return EmptyMessage, nil
}

func (s streamSource) Commit(v interface{}) error {
	return nil
}

func (s streamSource) Close() error {
	return nil
}

type streamProcessor struct{}

func (p streamProcessor) WithPipe(Pipe) {}

func (p streamProcessor) Process(msg Message) error {
	return nil
}

func (p streamProcessor) Close() error {
	return nil
}
