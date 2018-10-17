package streams_test

import (
	"testing"

	"github.com/msales/streams"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestProcessorPipe_Duration(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	child1 := new(MockPump)
	child1.On("Process", msg).Return(nil)
	child2 := new(MockPump)
	child2.On("Process", msg).Return(nil)
	pipe := streams.NewPipe([]streams.Pump{child1, child2})
	tPipe := pipe.(streams.TimedPipe)

	err := pipe.Forward(msg)
	assert.NoError(t, err)

	d := tPipe.Duration()

	if d == 0 {
		assert.Fail(t, "Pipe Duration returned 0")
	}
}

func TestProcessorPipe_Reset(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	child1 := new(MockPump)
	child1.On("Process", msg).Return(nil)
	child2 := new(MockPump)
	child2.On("Process", msg).Return(nil)
	pipe := streams.NewPipe([]streams.Pump{child1, child2})
	tPipe := pipe.(streams.TimedPipe)

	err := pipe.Forward(msg)
	assert.NoError(t, err)
	tPipe.Reset()

	d := tPipe.Duration()

	if d != 0 {
		assert.Fail(t, "Pipe Duration did not return 0")
	}
}

func TestProcessorPipe_Forward(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	child1 := new(MockPump)
	child1.On("Process", msg).Return(nil)
	child2 := new(MockPump)
	child2.On("Process", msg).Return(nil)
	pipe := streams.NewPipe([]streams.Pump{child1, child2})

	err := pipe.Forward(msg)

	assert.NoError(t, err)
	child1.AssertExpectations(t)
	child2.AssertExpectations(t)
}

func TestProcessorPipe_ForwardError(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	child1 := new(MockPump)
	child1.On("Process", msg).Return(errors.New("test"))
	pipe := streams.NewPipe([]streams.Pump{child1})

	err := pipe.Forward(msg)

	assert.Error(t, err)
	child1.AssertExpectations(t)
}

func TestProcessorPipe_ForwardToChild(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	child1 := new(MockPump)
	child2 := new(MockPump)
	child2.On("Process", msg).Return(nil)
	pipe := streams.NewPipe([]streams.Pump{child1, child2})

	err := pipe.ForwardToChild(msg, 1)

	assert.NoError(t, err)
	child2.AssertExpectations(t)
}

func TestProcessorPipe_ForwardToChildIndexError(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	child1 := new(MockPump)
	child1.On("Process", msg).Return(errors.New("test"))
	pipe := streams.NewPipe([]streams.Pump{child1})

	err := pipe.ForwardToChild(msg, 1)

	assert.Error(t, err)
}

func TestProcessorPipe_ForwardToChildError(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	pipe := streams.NewPipe([]streams.Pump{})

	err := pipe.ForwardToChild(msg, 1)

	assert.Error(t, err)
}

func TestProcessorPipe_Commit(t *testing.T) {
	src := new(MockSource)
	src.On("Commit", interface{}("test")).Return(nil)
	msg := streams.NewMessage(nil, nil).WithMetadata(src, "test")
	pipe := streams.NewPipe([]streams.Pump{})

	err := pipe.Commit(msg)

	assert.NoError(t, err)
	src.AssertExpectations(t)
}

func TestProcessorPipe_CommitWithError(t *testing.T) {
	src := new(MockSource)
	src.On("Commit", interface{}("test")).Return(errors.New("test"))
	msg := streams.NewMessage(nil, nil).WithMetadata(src, "test")
	pipe := streams.NewPipe([]streams.Pump{})

	err := pipe.Commit(msg)

	assert.Error(t, err)
	src.AssertExpectations(t)
}
