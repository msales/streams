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
	n := ProcessorNode{processor: &testProcessor{}}

	n.WithContext(ctx)

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
	key := "test"
	value := "test"

	ctx := mocks.NewContext(t)
	ctx.ExpectForward(key, value)

	n := ProcessorNode{processor: &testProcessor{}}
	n.WithContext(ctx)

	n.Process(key, value)

	ctx.AssertExpectations()
}

func TestProcessorNode_ProcessWithError(t *testing.T) {
	key := "test"
	value := "test"

	ctx := mocks.NewContext(t)
	ctx.ExpectForward(key, value)

	n := ProcessorNode{processor: &testProcessor{shouldError: true}}
	n.WithContext(ctx)

	err := n.Process(key, value)

	assert.Error(t, err)
}

func TestProcessorNode_Close(t *testing.T) {
	p := &testProcessor{}
	n := ProcessorNode{processor: p}

	err := n.Close()

	assert.NoError(t, err)
	assert.True(t, p.closeCalled)
}

type testProcessor struct {
	ctx Context

	shouldError bool
	closeCalled bool
}

func (p *testProcessor) WithContext(ctx Context) {
	p.ctx = ctx
}

func (p *testProcessor) Process(key, value interface{}) error {
	if p.shouldError {
		return errors.New("test")
	}

	p.ctx.Forward(key, value)
	return nil
}

func (p *testProcessor) Close() error {
	p.closeCalled = true
	return nil
}
