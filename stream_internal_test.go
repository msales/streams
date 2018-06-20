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

	top := builder.Build()

	assert.Len(t, top.Sources(), 1)
	assert.Len(t, top.Processors(), 2)
	assert.Contains(t, top.Sources(), source)
}

func TestStream_Filter(t *testing.T) {
	source := &streamSource{}
	builder := NewStreamBuilder()

	stream := builder.Source("source", source).Filter("test", func(msg *Message) (bool, error) {
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
		func(msg *Message) (bool, error) {
			return true, nil
		},
		func(msg *Message) (bool, error) {
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
		Map("test", func(msg *Message) (*Message, error) {
			return nil, nil
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
		FlatMap("test", func(msg *Message) ([]*Message, error) {
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
	assert.IsType(t, &PassThroughProcessor{}, stream.parents[0].(*ProcessorNode).processor)
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

type streamSource struct{}

func (s streamSource) Consume() (*Message, error) {
	return nil, nil
}

func (s streamSource) Commit() error {
	return nil
}

func (s streamSource) Close() error {
	return nil
}

type streamProcessor struct{}

func (p streamProcessor) WithPipe(Pipe) {}

func (p streamProcessor) Process(msg *Message) error {
	return nil
}

func (p streamProcessor) Close() error {
	return nil
}
