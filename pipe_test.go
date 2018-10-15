package streams_test

import (
	"testing"

	"github.com/msales/streams"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestProcessorPipe_Forward(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	child1 := new(MockPump)
	child1.On("Process", msg).Return(nil)
	child2 := new(MockPump)
	child2.On("Process", msg).Return(nil)
	pipe := streams.NewProcessorPipe([]streams.Pump{child1, child2})

	pipe.Forward(msg)

	child1.AssertExpectations(t)
	child2.AssertExpectations(t)
}

func TestProcessorPipe_ForwardToChild(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	child1 := new(MockPump)
	child2 := new(MockPump)
	child2.On("Process", msg).Return(nil)
	pipe := streams.NewProcessorPipe([]streams.Pump{child1, child2})

	pipe.ForwardToChild(msg, 1)

	child2.AssertExpectations(t)
}

func TestProcessorPipe_ForwardToChildIndexError(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	pipe := streams.NewProcessorPipe([]streams.Pump{})

	err := pipe.ForwardToChild(msg, 1)

	assert.Error(t, err)
}

func TestProcessorPipe_Commit(t *testing.T) {
	src := new(MockSource)
	src.On("Commit", interface{}("test")).Return(nil)
	msg := streams.NewMessage(nil, nil).WithMetadata(src, "test")
	pipe := streams.NewProcessorPipe([]streams.Pump{})

	err := pipe.Commit(msg)

	assert.NoError(t, err)
	src.AssertExpectations(t)
}

func TestProcessorPipe_CommitWithError(t *testing.T) {
	src := new(MockSource)
	src.On("Commit", interface{}("test")).Return(errors.New("test"))
	msg := streams.NewMessage(nil, nil).WithMetadata(src, "test")
	pipe := streams.NewProcessorPipe([]streams.Pump{})

	err := pipe.Commit(msg)

	assert.Error(t, err)
	src.AssertExpectations(t)
}
