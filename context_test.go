package streams_test

import (
	"testing"

	"github.com/msales/streams"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestProcessorContext_Forward(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	child := new(MockNode)
	child.On("Process", msg).Return(nil)
	parent := new(MockNode)
	parent.On("Children").Return([]streams.Node{child})
	task := new(MockTask)
	ctx := streams.NewProcessorContext(task)
	ctx.SetNode(parent)

	ctx.Forward(msg)

	child.AssertExpectations(t)
}

func TestProcessorContext_ForwardReturnsChildError(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	child := new(MockNode)
	child.On("Process", msg).Return(errors.New("test"))
	parent := new(MockNode)
	parent.On("Children").Return([]streams.Node{child})
	task := new(MockTask)
	ctx := streams.NewProcessorContext(task)
	ctx.SetNode(parent)

	err := ctx.Forward(msg)

	child.AssertExpectations(t)
	assert.Error(t, err)
}

func TestProcessorContext_ForwardToChild(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	child := new(MockNode)
	child.On("Process", msg).Return(nil)
	parent := new(MockNode)
	parent.On("Children").Return([]streams.Node{nil, child})
	task := new(MockTask)
	ctx := streams.NewProcessorContext(task)
	ctx.SetNode(parent)

	ctx.ForwardToChild(msg, 1)

	child.AssertExpectations(t)
}

func TestProcessorContext_ForwardToChildIndexError(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	parent := new(MockNode)
	parent.On("Children").Return([]streams.Node{})
	task := new(MockTask)
	ctx := streams.NewProcessorContext(task)
	ctx.SetNode(parent)

	err := ctx.ForwardToChild(msg, 1)

	assert.Error(t, err)
}

func TestProcessorContext_ForwardToChildReturnsChildError(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	child := new(MockNode)
	child.On("Process", msg).Return(errors.New("test"))
	parent := new(MockNode)
	parent.On("Children").Return([]streams.Node{nil, child})
	task := new(MockTask)
	ctx := streams.NewProcessorContext(task)
	ctx.SetNode(parent)

	err := ctx.ForwardToChild(msg, 1)

	child.AssertExpectations(t)
	assert.Error(t, err)
}

func TestProcessorContext_Commit(t *testing.T) {
	task := new(MockTask)
	task.On("Commit").Return(nil)
	ctx := streams.NewProcessorContext(task)

	err := ctx.Commit()

	assert.NoError(t, err)
	task.AssertExpectations(t)
}

func TestProcessorContext_CommitWithError(t *testing.T) {
	task := new(MockTask)
	task.On("Commit").Return(errors.New("test"))
	ctx := streams.NewProcessorContext(task)

	err := ctx.Commit()

	assert.Error(t, err)
	task.AssertExpectations(t)
}
