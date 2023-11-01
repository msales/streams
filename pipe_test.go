package streams_test

import (
	"errors"
	"testing"

	"github.com/msales/streams/v7"
	"github.com/stretchr/testify/assert"
)

func TestProcessorPipe_Duration(t *testing.T) {
	store := new(streams.MockMetastore)
	supervisor := new(streams.MockSupervisor)
	msg := streams.NewMessage("test", "test")
	child1 := new(streams.MockPump)
	child1.On("Accept", msg).Return(nil)
	child2 := new(streams.MockPump)
	child2.On("Accept", msg).Return(nil)
	pipe := streams.NewPipe(store, supervisor, nil, []streams.Pump{child1, child2})
	tPipe := pipe.(streams.TimedPipe)

	err := pipe.Forward(msg)
	assert.NoError(t, err)

	d := tPipe.Duration()

	if d == 0 {
		assert.Fail(t, "Pipe Duration returned 0")
	}
}

func TestProcessorPipe_Reset(t *testing.T) {
	store := new(streams.MockMetastore)
	supervisor := new(streams.MockSupervisor)
	msg := streams.NewMessage("test", "test")
	child1 := new(streams.MockPump)
	child1.On("Accept", msg).Return(nil)
	child2 := new(streams.MockPump)
	child2.On("Accept", msg).Return(nil)
	pipe := streams.NewPipe(store, supervisor, nil, []streams.Pump{child1, child2})
	tPipe := pipe.(streams.TimedPipe)

	err := pipe.Forward(msg)
	assert.NoError(t, err)
	tPipe.Reset()

	d := tPipe.Duration()

	if d != 0 {
		assert.Fail(t, "Pipe Duration did not return 0")
	}
}

func TestProcessorPipe_Mark(t *testing.T) {
	proc := new(streams.MockProcessor)
	src := new(streams.MockSource)
	store := new(streams.MockMetastore)
	supervisor := new(streams.MockSupervisor)
	meta := new(streams.MockMetadata)
	store.On("Mark", proc, src, meta).Return(errors.New("test"))
	msg := streams.NewMessage("test", "test").WithMetadata(src, meta)
	pipe := streams.NewPipe(store, supervisor, proc, []streams.Pump{})

	err := pipe.Mark(msg)

	assert.Error(t, err)
	store.AssertExpectations(t)
}

func TestProcessorPipe_Forward(t *testing.T) {
	store := new(streams.MockMetastore)
	supervisor := new(streams.MockSupervisor)
	msg := streams.NewMessage("test", "test")
	child1 := new(streams.MockPump)
	child1.On("Accept", msg).Return(nil)
	child2 := new(streams.MockPump)
	child2.On("Accept", msg).Return(nil)
	pipe := streams.NewPipe(store, supervisor, nil, []streams.Pump{child1, child2})

	err := pipe.Forward(msg)

	assert.NoError(t, err)
	child1.AssertExpectations(t)
	child2.AssertExpectations(t)
}

func TestProcessorPipe_ForwardError(t *testing.T) {
	store := new(streams.MockMetastore)
	supervisor := new(streams.MockSupervisor)
	msg := streams.NewMessage("test", "test")
	child1 := new(streams.MockPump)
	child1.On("Accept", msg).Return(errors.New("test"))
	pipe := streams.NewPipe(store, supervisor, nil, []streams.Pump{child1})

	err := pipe.Forward(msg)

	assert.Error(t, err)
	child1.AssertExpectations(t)
}

func TestProcessorPipe_ForwardToChild(t *testing.T) {
	store := new(streams.MockMetastore)
	supervisor := new(streams.MockSupervisor)
	msg := streams.NewMessage("test", "test")
	child1 := new(streams.MockPump)
	child2 := new(streams.MockPump)
	child2.On("Accept", msg).Return(nil)
	pipe := streams.NewPipe(store, supervisor, nil, []streams.Pump{child1, child2})

	err := pipe.ForwardToChild(msg, 1)

	assert.NoError(t, err)
	child2.AssertExpectations(t)
}

func TestProcessorPipe_ForwardToChildIndexError(t *testing.T) {
	store := new(streams.MockMetastore)
	supervisor := new(streams.MockSupervisor)
	msg := streams.NewMessage("test", "test")
	child1 := new(streams.MockPump)
	child1.On("Accept", msg).Return(errors.New("test"))
	pipe := streams.NewPipe(store, supervisor, nil, []streams.Pump{child1})

	err := pipe.ForwardToChild(msg, 1)

	assert.Error(t, err)
}

func TestProcessorPipe_ForwardToChildError(t *testing.T) {
	store := new(streams.MockMetastore)
	supervisor := new(streams.MockSupervisor)
	msg := streams.NewMessage("test", "test")
	pipe := streams.NewPipe(store, supervisor, nil, []streams.Pump{})

	err := pipe.ForwardToChild(msg, 1)

	assert.Error(t, err)
}

func TestProcessorPipe_Commit(t *testing.T) {
	src := new(streams.MockSource)
	store := new(streams.MockMetastore)
	supervisor := new(streams.MockSupervisor)
	meta := new(streams.MockMetadata)
	proc := new(streams.MockProcessor)
	store.On("Mark", proc, src, meta).Return(nil)
	supervisor.On("Commit", proc).Return(nil)
	msg := streams.NewMessage(nil, nil).WithMetadata(src, meta)
	pipe := streams.NewPipe(store, supervisor, proc, []streams.Pump{})

	err := pipe.Commit(msg)

	assert.NoError(t, err)
	store.AssertExpectations(t)
}

func TestProcessorPipe_CommitMarkError(t *testing.T) {
	src := new(streams.MockSource)
	store := new(streams.MockMetastore)
	supervisor := new(streams.MockSupervisor)
	meta := new(streams.MockMetadata)
	proc := new(streams.MockProcessor)
	store.On("Mark", proc, src, meta).Return(errors.New("test"))
	msg := streams.NewMessage(nil, nil).WithMetadata(src, meta)
	pipe := streams.NewPipe(store, supervisor, proc, []streams.Pump{})

	err := pipe.Commit(msg)

	assert.Error(t, err)
	store.AssertExpectations(t)
}

func TestProcessorPipe_CommitSupervisorError(t *testing.T) {
	src := new(streams.MockSource)
	store := new(streams.MockMetastore)
	supervisor := new(streams.MockSupervisor)
	meta := new(streams.MockMetadata)
	proc := new(streams.MockProcessor)
	store.On("Mark", proc, src, meta).Return(nil)
	supervisor.On("Commit", proc).Return(errors.New("test"))
	msg := streams.NewMessage(nil, nil).WithMetadata(src, meta)
	pipe := streams.NewPipe(store, supervisor, proc, []streams.Pump{})

	err := pipe.Commit(msg)

	assert.Error(t, err)
	store.AssertExpectations(t)
}

func BenchmarkProcessorPipe_Mark(b *testing.B) {
	store := &fakeMetastore{}
	supervisor := &fakeSupervisor{}
	proc := &fakeCommitter{}
	msg := streams.NewMessage(nil, "test")
	pipe := streams.NewPipe(store, supervisor, proc, []streams.Pump{})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = pipe.Mark(msg)
	}
}

func BenchmarkProcessorPipe_Commit(b *testing.B) {
	store := &fakeMetastore{}
	supervisor := &fakeSupervisor{}
	proc := &fakeCommitter{}
	msg := streams.NewMessage(nil, "test")
	pipe := streams.NewPipe(store, supervisor, proc, []streams.Pump{})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = pipe.Commit(msg)
	}
}
