package streams

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestProcessorContext_Forward(t *testing.T) {
	child := new(MockNode)
	child.On("Process", nil, "test", "test").Return(nil)
	parent := new(MockNode)
	parent.On("Children").Return([]Node{child})
	task := new(MockTask)
	ctx := NewProcessorContext(task)
	ctx.currentNode = parent

	ctx.Forward(nil, "test", "test")

	child.AssertExpectations(t)
}

func TestProcessorContext_ForwardReturnsChildError(t *testing.T) {
	child := new(MockNode)
	child.On("Process", nil, "test", "test").Return(errors.New("test"))
	parent := new(MockNode)
	parent.On("Children").Return([]Node{child})
	task := new(MockTask)
	ctx := NewProcessorContext(task)
	ctx.currentNode = parent

	err := ctx.Forward(nil, "test", "test")

	child.AssertExpectations(t)
	assert.Error(t, err)
}

func TestProcessorContext_ForwardToChild(t *testing.T) {
	child := new(MockNode)
	child.On("Process", nil, "test", "test").Return(nil)
	parent := new(MockNode)
	parent.On("Children").Return([]Node{nil, child})
	task := new(MockTask)
	ctx := NewProcessorContext(task)
	ctx.currentNode = parent

	ctx.ForwardToChild(nil, "test", "test", 1)

	child.AssertExpectations(t)
}

func TestProcessorContext_ForwardToChildIndexError(t *testing.T) {
	parent := new(MockNode)
	parent.On("Children").Return([]Node{})
	task := new(MockTask)
	ctx := NewProcessorContext(task)
	ctx.currentNode = parent

	err := ctx.ForwardToChild(nil, "test", "test", 1)

	assert.Error(t, err)
}

func TestProcessorContext_ForwardToChildReturnsChildError(t *testing.T) {
	child := new(MockNode)
	child.On("Process", nil, "test", "test").Return(errors.New("test"))
	parent := new(MockNode)
	parent.On("Children").Return([]Node{nil, child})
	task := new(MockTask)
	ctx := NewProcessorContext(task)
	ctx.currentNode = parent

	err := ctx.ForwardToChild(nil, "test", "test", 1)

	child.AssertExpectations(t)
	assert.Error(t, err)
}

func TestProcessorContext_Commit(t *testing.T) {
	task := new(MockTask)
	task.On("Commit").Return(nil)
	ctx := NewProcessorContext(task)

	err := ctx.Commit()

	assert.NoError(t, err)
	task.AssertExpectations(t)
}

func TestProcessorContext_CommitWithError(t *testing.T) {
	task := new(MockTask)
	task.On("Commit").Return(errors.New("test"))
	ctx := NewProcessorContext(task)

	err := ctx.Commit()

	assert.Error(t, err)
	task.AssertExpectations(t)
}
