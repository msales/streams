package streams_test

import (
	"testing"

	"github.com/msales/streams"
	"github.com/msales/streams/mocks"
	"github.com/pkg/errors"
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

func TestSourceNode_Process(t *testing.T) {
	key := "test"
	value := "test"

	pipe := mocks.NewPipe(t)
	pipe.ExpectForward(key, value)

	n := streams.SourceNode{}
	n.WithPipe(pipe)

	n.Process(streams.NewMessage(key, value))

	pipe.AssertExpectations()
}

func TestSourceNode_Close(t *testing.T) {
	n := streams.SourceNode{}

	err := n.Close()

	assert.NoError(t, err)
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

func TestProcessorNode_Process(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	pipe := mocks.NewPipe(t)
	p := new(MockProcessor)
	p.On("WithPipe", pipe).Return(nil)
	p.On("Process", msg).Return(nil)
	n := streams.NewProcessorNode("test", p)
	n.WithPipe(pipe)

	err := n.Process(msg)

	assert.NoError(t, err)
	p.AssertExpectations(t)
}

func TestProcessorNode_ProcessWithError(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	pipe := mocks.NewPipe(t)
	p := new(MockProcessor)
	p.On("WithPipe", pipe).Return(nil)
	p.On("Process", msg).Return(errors.New("test"))
	n := streams.NewProcessorNode("test", p)
	n.WithPipe(pipe)

	err := n.Process(msg)

	assert.Error(t, err)
	p.AssertExpectations(t)
}

func TestProcessorNode_Close(t *testing.T) {
	p := new(MockProcessor)
	p.On("Close").Return(nil)
	n := streams.NewProcessorNode("test", p)

	err := n.Close()

	assert.NoError(t, err)
	p.AssertExpectations(t)
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
	assert.Len(t, to.Processors(), 1)
	assert.Equal(t, n, to.Processors()[0])
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
