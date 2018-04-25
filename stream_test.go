package streams

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStreamBuilder_Source(t *testing.T) {
	source := new(MockSource)
	builder := NewStreamBuilder()

	stream := builder.Source("test", source)

	assert.Len(t, stream.parents, 1)
	assert.IsType(t, &SourceNode{}, stream.parents[0])
	assert.IsType(t, stream.parents[0].(*SourceNode).name, "test")
}

func TestStreamBuilder_Build(t *testing.T) {
	proc := new(MockProcessor)
	source := new(MockSource)
	builder := NewStreamBuilder()
	builder.Source("test", source).Process("test1", proc)

	top := builder.Build()

	assert.Len(t, top.Sources(), 1)
	assert.Len(t, top.Processors(), 2)
	assert.Contains(t, top.Sources(), source)
}

func TestStream_Filter(t *testing.T) {
	source := new(MockSource)
	builder := NewStreamBuilder()


	stream := builder.Source("source", source).Filter("test", func(k, v interface{}) (bool, error) {
		return true, nil
	})

	assert.Len(t, stream.parents, 1)
	assert.IsType(t, &ProcessorNode{}, stream.parents[0])
	assert.Equal(t, stream.parents[0].(*ProcessorNode).name, "test")
	assert.IsType(t, &FilterProcessor{}, stream.parents[0].(*ProcessorNode).processor)
}

func TestStream_Print(t *testing.T) {
	source := new(MockSource)
	builder := NewStreamBuilder()


	stream := builder.Source("source", source).Print("test")

	assert.Len(t, stream.parents, 1)
	assert.IsType(t, &ProcessorNode{}, stream.parents[0])
	assert.Equal(t, stream.parents[0].(*ProcessorNode).name, "test")
	assert.IsType(t, &PrintProcessor{}, stream.parents[0].(*ProcessorNode).processor)
}

func TestStream_Process(t *testing.T) {
	proc := new(MockProcessor)
	source := new(MockSource)
	builder := NewStreamBuilder()


	stream := builder.Source("source", source).Process("test", proc)

	assert.Len(t, stream.parents, 1)
	assert.IsType(t, &ProcessorNode{}, stream.parents[0])
	assert.Equal(t, stream.parents[0].(*ProcessorNode).name, "test")
	assert.Equal(t, stream.parents[0].(*ProcessorNode).processor, proc)
}
