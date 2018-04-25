package streams

import (
	"testing"

	"github.com/msales/streams/mocks"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestSourceNode_WithContext(t *testing.T) {
	ctx := &mocks.Context{}
	n := SourceNode{}

	n.WithContext(ctx)

	assert.Equal(t, ctx, n.ctx)
}

func TestSourceNode_AddChild(t *testing.T) {
	child := &ProcessorNode{}
	n := SourceNode{}

	n.AddChild(child)

	assert.Len(t, n.children, 1)
	assert.Equal(t, child, n.children[0])
}

func TestSourceNode_Children(t *testing.T) {
	child := &ProcessorNode{}
	n := SourceNode{}
	n.AddChild(child)

	children := n.Children()

	assert.Len(t, children, 1)
	assert.Equal(t, child, children[0])
}

func TestSourceNode_Process(t *testing.T) {
	key := "test"
	value := "test"

	ctx := mocks.NewContext(t)
	ctx.ExpectForward(key, value)

	n := SourceNode{}
	n.WithContext(ctx)

	n.Process(nil, nil)
	n.Process(key, value)

	ctx.AssertExpectations()
}

func TestSourceNode_Close(t *testing.T) {
	n := SourceNode{}

	err := n.Close()

	assert.NoError(t, err)
}

func TestProcessorNode_WithContext(t *testing.T) {
	ctx := &mocks.Context{}
	p := new(MockProcessor)
	p.On("WithContext", ctx)
	n := ProcessorNode{processor: p}

	n.WithContext(ctx)

	p.AssertExpectations(t)
	assert.Equal(t, ctx, n.ctx)
}

func TestProcessorNode_AddChild(t *testing.T) {
	child := &ProcessorNode{}
	n := ProcessorNode{}

	n.AddChild(child)

	assert.Len(t, n.children, 1)
	assert.Equal(t, child, n.children[0])
}

func TestProcessorNode_Children(t *testing.T) {
	child := &ProcessorNode{}
	n := ProcessorNode{}
	n.AddChild(child)

	children := n.Children()

	assert.Len(t, children, 1)
	assert.Equal(t, child, children[0])
}

func TestProcessorNode_Process(t *testing.T) {
	ctx := &mocks.Context{}
	p := new(MockProcessor)
	p.On("Process", "test", "test").Return(nil)
	n := ProcessorNode{processor: p}
	n.ctx = ctx

	err := n.Process("test", "test")

	assert.NoError(t, err)
	p.AssertExpectations(t)
}

func TestProcessorNode_ProcessWithError(t *testing.T) {
	ctx := &mocks.Context{}
	p := new(MockProcessor)
	p.On("Process", "test", "test").Return(errors.New("test"))
	n := ProcessorNode{processor: p}
	n.ctx = ctx

	err := n.Process("test", "test")

	assert.Error(t, err)
	p.AssertExpectations(t)
}

func TestProcessorNode_Close(t *testing.T) {
	p := new(MockProcessor)
	p.On("Close").Return(nil)
	n := ProcessorNode{processor: p}

	err := n.Close()

	assert.NoError(t, err)
	p.AssertExpectations(t)
}

func TestTopology_Sources(t *testing.T) {
	s := new(MockSource)
	n := &SourceNode{}
	to := &Topology{
		sources: map[Source]Node{s: n},
	}

	sources := to.Sources()

	assert.Len(t, sources, 1)
	assert.Equal(t, n, sources[s])
}

func TestTopology_Processors(t *testing.T) {
	n := &SourceNode{}
	to := &Topology{
		processors: []Node{n},
	}

	processors := to.Processors()

	assert.Len(t, processors, 1)
	assert.Equal(t, n, processors[0])
}

func TestTopologyBuilder_AddSource(t *testing.T) {
	s := new(MockSource)
	tb := NewTopologyBuilder()

	n := tb.AddSource("test", s)
	to := tb.Build()

	assert.IsType(t, &SourceNode{}, n)
	assert.Equal(t, "test", n.(*SourceNode).name)
	assert.Len(t, to.Sources(), 1)
	assert.Equal(t, n, to.Sources()[s])
	assert.Len(t, to.Processors(), 1)
	assert.Equal(t, n, to.Processors()[0])
}

func TestTopologyBuilder_AddProcessor(t *testing.T) {
	p := new(MockProcessor)
	pn := &ProcessorNode{}
	tb := NewTopologyBuilder()

	n := tb.AddProcessor("test", p, []Node{pn})
	to := tb.Build()

	assert.IsType(t, &ProcessorNode{}, n)
	assert.Equal(t, "test", n.(*ProcessorNode).name)
	assert.Len(t, pn.Children(), 1)
	assert.Equal(t, n, pn.Children()[0])
	assert.Len(t, to.Processors(), 1)
	assert.Equal(t, n, to.Processors()[0])
}
