package streams_test

import (
	"testing"

	"github.com/msales/streams"
	"github.com/stretchr/testify/assert"
)

func TestNewSourceNode(t *testing.T) {
	n := streams.NewSourceNode("test")

	assert.Equal(t, "test", n.Name())
}

func TestSourceNode_AddChild(t *testing.T) {
	child := &streams.ProcessorNode{}
	n := streams.SourceNode{}

	n.AddChild(child)

	assert.Len(t, n.Children(), 1)
	assert.Equal(t, child, n.Children()[0])
}

func TestSourceNode_Children(t *testing.T) {
	child := &streams.ProcessorNode{}
	n := streams.SourceNode{}
	n.AddChild(child)

	children := n.Children()

	assert.Len(t, children, 1)
	assert.Equal(t, child, children[0])
}

func TestSourceNode_Processor(t *testing.T) {
	n := streams.SourceNode{}

	processor := n.Processor()

	assert.Nil(t, processor)
}

func TestNewProcessorNode(t *testing.T) {
	p := new(MockProcessor)
	n := streams.NewProcessorNode("test", p)

	assert.Equal(t, "test", n.Name())
}

func TestProcessorNode_AddChild(t *testing.T) {
	child := &streams.ProcessorNode{}
	n := streams.ProcessorNode{}

	n.AddChild(child)

	assert.Len(t, n.Children(), 1)
	assert.Equal(t, child, n.Children()[0])
}

func TestProcessorNode_Children(t *testing.T) {
	child := &streams.ProcessorNode{}
	n := streams.ProcessorNode{}
	n.AddChild(child)

	children := n.Children()

	assert.Len(t, children, 1)
	assert.Equal(t, child, children[0])
}

func TestProcessorNode_Processor(t *testing.T) {
	p := new(MockProcessor)
	n := streams.NewProcessorNode("test", p)

	processor := n.Processor()

	assert.Exactly(t, p, processor)
}

func TestTopologyBuilder_AddSource(t *testing.T) {
	s := new(MockSource)
	tb := streams.NewTopologyBuilder()

	n := tb.AddSource("test", s)
	to := tb.Build()

	assert.IsType(t, &streams.SourceNode{}, n)
	assert.Equal(t, "test", n.(*streams.SourceNode).Name())
	assert.Len(t, to.Sources(), 1)
	assert.Equal(t, n, to.Sources()[s])
}

func TestTopologyBuilder_AddProcessor(t *testing.T) {
	p := new(MockProcessor)
	pn := &streams.ProcessorNode{}
	tb := streams.NewTopologyBuilder()

	n := tb.AddProcessor("test", p, []streams.Node{pn})
	to := tb.Build()

	assert.IsType(t, &streams.ProcessorNode{}, n)
	assert.Equal(t, "test", n.(*streams.ProcessorNode).Name())
	assert.Len(t, pn.Children(), 1)
	assert.Equal(t, n, pn.Children()[0])
	assert.Len(t, to.Processors(), 1)
	assert.Equal(t, n, to.Processors()[0])
}

func TestTopology_Sources(t *testing.T) {
	s := new(MockSource)
	tb := streams.NewTopologyBuilder()
	sn := tb.AddSource("test", s)
	to := tb.Build()

	sources := to.Sources()

	assert.Len(t, sources, 1)
	assert.Equal(t, sn, sources[s])
}

func TestTopology_Processors(t *testing.T) {
	p := new(MockProcessor)
	tb := streams.NewTopologyBuilder()
	pn := tb.AddProcessor("test2", p, []streams.Node{})
	to := tb.Build()

	processors := to.Processors()

	assert.Len(t, processors, 1)
	assert.Equal(t, pn, processors[0])
}
