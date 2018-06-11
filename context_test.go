package streams

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestProcessorContext_Forward(t *testing.T) {
	child := new(MockNode)
	child.On("Process", "test", "test").Return(nil)
	parent := new(MockNode)
	parent.On("Children").Return([]Node{child})
	task := new(MockTask)
	ctx := NewProcessorContext(context.Background(), task)
	ctx.currentNode = parent

	ctx.Forward("test", "test")

	child.AssertExpectations(t)
}

func TestProcessorContext_ForwardReturnsChildError(t *testing.T) {
	child := new(MockNode)
	child.On("Process", "test", "test").Return(errors.New("test"))
	parent := new(MockNode)
	parent.On("Children").Return([]Node{child})
	task := new(MockTask)
	ctx := NewProcessorContext(context.Background(), task)
	ctx.currentNode = parent

	err := ctx.Forward("test", "test")

	child.AssertExpectations(t)
	assert.Error(t, err)
}

func TestProcessorContext_ForwardToChild(t *testing.T) {
	child := new(MockNode)
	child.On("Process", "test", "test").Return(nil)
	parent := new(MockNode)
	parent.On("Children").Return([]Node{nil, child})
	task := new(MockTask)
	ctx := NewProcessorContext(context.Background(), task)
	ctx.currentNode = parent

	ctx.ForwardToChild("test", "test", 1)

	child.AssertExpectations(t)
}

func TestProcessorContext_ForwardToChildIndexError(t *testing.T) {
	parent := new(MockNode)
	parent.On("Children").Return([]Node{})
	task := new(MockTask)
	ctx := NewProcessorContext(context.Background(), task)
	ctx.currentNode = parent

	err := ctx.ForwardToChild("test", "test", 1)

	assert.Error(t, err)
}

func TestProcessorContext_ForwardToChildReturnsChildError(t *testing.T) {
	child := new(MockNode)
	child.On("Process", "test", "test").Return(errors.New("test"))
	parent := new(MockNode)
	parent.On("Children").Return([]Node{nil, child})
	task := new(MockTask)
	ctx := NewProcessorContext(context.Background(), task)
	ctx.currentNode = parent

	err := ctx.ForwardToChild("test", "test", 1)

	child.AssertExpectations(t)
	assert.Error(t, err)
}

func TestProcessorContext_Commit(t *testing.T) {
	task := new(MockTask)
	task.On("Commit").Return(nil)
	ctx := NewProcessorContext(context.Background(), task)

	err := ctx.Commit()

	assert.NoError(t, err)
	task.AssertExpectations(t)
}

func TestProcessorContext_CommitWithError(t *testing.T) {
	task := new(MockTask)
	task.On("Commit").Return(errors.New("test"))
	ctx := NewProcessorContext(context.Background(), task)

	err := ctx.Commit()

	assert.Error(t, err)
	task.AssertExpectations(t)
}
