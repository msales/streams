package streams_test

import (
	"testing"

	"github.com/msales/streams"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestProcessorPipe_Forward(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	child := new(MockNode)
	child.On("Process", msg).Return(nil)
	parent := new(MockNode)
	parent.On("Children").Return([]streams.Node{child})
	ctx := streams.NewProcessorPipe()
	ctx.SetNode(parent)

	ctx.Forward(msg)

	child.AssertExpectations(t)
}

func TestProcessorPipe_ForwardReturnsChildError(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	child := new(MockNode)
	child.On("Process", msg).Return(errors.New("test"))
	parent := new(MockNode)
	parent.On("Children").Return([]streams.Node{child})
	ctx := streams.NewProcessorPipe()
	ctx.SetNode(parent)

	err := ctx.Forward(msg)

	child.AssertExpectations(t)
	assert.Error(t, err)
}

func TestProcessorPipe_ForwardToChild(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	child := new(MockNode)
	child.On("Process", msg).Return(nil)
	parent := new(MockNode)
	parent.On("Children").Return([]streams.Node{nil, child})
	ctx := streams.NewProcessorPipe()
	ctx.SetNode(parent)

	ctx.ForwardToChild(msg, 1)

	child.AssertExpectations(t)
}

func TestProcessorPipe_ForwardToChildIndexError(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	parent := new(MockNode)
	parent.On("Children").Return([]streams.Node{})
	ctx := streams.NewProcessorPipe()
	ctx.SetNode(parent)

	err := ctx.ForwardToChild(msg, 1)

	assert.Error(t, err)
}

func TestProcessorPipe_ForwardToChildReturnsChildError(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	child := new(MockNode)
	child.On("Process", msg).Return(errors.New("test"))
	parent := new(MockNode)
	parent.On("Children").Return([]streams.Node{nil, child})
	ctx := streams.NewProcessorPipe()
	ctx.SetNode(parent)

	err := ctx.ForwardToChild(msg, 1)

	child.AssertExpectations(t)
	assert.Error(t, err)
}

func TestProcessorPipe_Commit(t *testing.T) {
	src := new(MockSource)
	src.On("Commit", interface{}("test")).Return(nil)
	msg := streams.NewMessage(nil, nil).WithMetadata(src, "test")
	ctx := streams.NewProcessorPipe()

	err := ctx.Commit(msg)

	assert.NoError(t, err)
	src.AssertExpectations(t)
}

func TestProcessorPipe_CommitWithError(t *testing.T) {
	src := new(MockSource)
	src.On("Commit", interface{}("test")).Return(errors.New("test"))
	msg := streams.NewMessage(nil, nil).WithMetadata(src, "test")
	ctx := streams.NewProcessorPipe()

	err := ctx.Commit(msg)

	assert.Error(t, err)
	src.AssertExpectations(t)
}
