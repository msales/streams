package streams_test

import (
	"testing"

	"github.com/msales/streams"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestProcessorPipe_Forward(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	child1 := new(MockNode)
	child2 := new(MockNode)
	parent := new(MockNode)
	parent.On("Children").Return([]streams.Node{child1, child2})
	pipe := streams.NewProcessorPipe(parent)

	pipe.Forward(msg)

	queue := pipe.Queue()
	assert.Len(t, queue, 2)
	assert.Exactly(t, child1, queue[0].Node)
	assert.Exactly(t, msg, queue[0].Msg)
	assert.Exactly(t, child2, queue[1].Node)
	assert.Exactly(t, msg, queue[1].Msg)
}

func TestProcessorPipe_ForwardToChild(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	child := new(MockNode)
	parent := new(MockNode)
	parent.On("Children").Return([]streams.Node{nil, child})
	pipe := streams.NewProcessorPipe(parent)

	pipe.ForwardToChild(msg, 1)

	queue := pipe.Queue()
	assert.Len(t, queue, 1)
	assert.Exactly(t, child, queue[0].Node)
	assert.Exactly(t, msg, queue[0].Msg)
}

func TestProcessorPipe_ForwardToChildIndexError(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	parent := new(MockNode)
	parent.On("Children").Return([]streams.Node{})
	pipe := streams.NewProcessorPipe(parent)

	err := pipe.ForwardToChild(msg, 1)

	assert.Error(t, err)
}

func TestProcessorPipe_Commit(t *testing.T) {
	src := new(MockSource)
	src.On("Commit", interface{}("test")).Return(nil)
	node := new(MockNode)
	msg := streams.NewMessage(nil, nil).WithMetadata(src, "test")
	pipe := streams.NewProcessorPipe(node)

	err := pipe.Commit(msg)

	assert.NoError(t, err)
	src.AssertExpectations(t)
}

func TestProcessorPipe_CommitWithError(t *testing.T) {
	src := new(MockSource)
	src.On("Commit", interface{}("test")).Return(errors.New("test"))
	node := new(MockNode)
	msg := streams.NewMessage(nil, nil).WithMetadata(src, "test")
	pipe := streams.NewProcessorPipe(node)

	err := pipe.Commit(msg)

	assert.Error(t, err)
	src.AssertExpectations(t)
}
