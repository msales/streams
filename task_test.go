package streams_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/msales/streams/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func passThroughMapper(msg streams.Message) (streams.Message, error) {
	return msg, nil
}

func TestNewTask(t *testing.T) {
	task := streams.NewTask(nil)

	assert.Implements(t, (*streams.Task)(nil), task)
}

func TestStreamTask_ConsumesAsyncMessages(t *testing.T) {
	msgs := make(chan streams.Message)
	msg := streams.NewMessage("test", "test")

	p := new(streams.MockProcessor)
	p.On("WithPipe", mock.Anything).Return(nil)
	p.On("Process", msg).Return(nil)
	p.On("Close").Return(nil)

	b := streams.NewStreamBuilder()
	b.Source("src", &streams.ChanSource{Msgs: msgs}).
		Map("pass-through", streams.MapperFunc(passThroughMapper)).
		Process("processor", p)

	tp, _ := b.Build()
	task := streams.NewTask(tp, streams.WithMode(streams.Async))
	task.OnError(func(err error) error {
		t.FailNow()
		return err
	})

	err := task.Start(context.Background())
	if err != nil {
		assert.FailNow(t, err.Error())
	}

	msgs <- msg

	time.Sleep(time.Millisecond)

	_ = task.Close()

	p.AssertExpectations(t)
}

func TestStreamTask_ConsumesSyncMessages(t *testing.T) {
	msgs := make(chan streams.Message)
	msg := streams.NewMessage("test", "test")

	p := new(streams.MockProcessor)
	p.On("WithPipe", mock.Anything).Return(nil)
	p.On("Process", msg).Return(nil)
	p.On("Close").Return(nil)

	b := streams.NewStreamBuilder()
	b.Source("src", &streams.ChanSource{Msgs: msgs}).
		Map("pass-through", streams.MapperFunc(passThroughMapper)).
		Process("processor", p)

	tp, _ := b.Build()
	task := streams.NewTask(tp, streams.WithMode(streams.Sync))
	task.OnError(func(err error) error {
		t.FailNow()
		return err
	})

	err := task.Start(context.Background())
	if err != nil {
		assert.FailNow(t, err.Error())
	}

	msgs <- msg

	time.Sleep(time.Millisecond)

	_ = task.Close()

	p.AssertExpectations(t)
}

func TestStreamTask_Throughput(t *testing.T) {
	msgs := make(chan streams.Message)
	msg := streams.NewMessage("test", "test")

	count := 0

	b := streams.NewStreamBuilder()
	b.Source("src", &streams.ChanSource{Msgs: msgs}).
		Map("pass-through", streams.MapperFunc(passThroughMapper)).
		Map("count", streams.MapperFunc(func(msg streams.Message) (streams.Message, error) {
			count++
			return msg, nil
		}))

	tp, _ := b.Build()
	task := streams.NewTask(tp)
	task.OnError(func(err error) error {
		t.FailNow()
		return err
	})

	err := task.Start(context.Background())
	if err != nil {
		assert.FailNow(t, err.Error())
	}

	for i := 0; i < 100; i++ {
		msgs <- msg
	}

	time.Sleep(time.Millisecond)

	_ = task.Close()

	assert.Equal(t, 100, count)
}

func TestStreamTask_CannotStartTwice(t *testing.T) {
	msgs := make(chan streams.Message)

	b := streams.NewStreamBuilder()
	b.Source("src", &streams.ChanSource{Msgs: msgs})

	tp, _ := b.Build()
	task := streams.NewTask(tp)
	task.OnError(func(err error) error {
		t.FailNow()
		return err
	})

	_ = task.Start(context.Background())

	err := task.Start(context.Background())

	_ = task.Close()

	assert.Error(t, err)
}

func TestStreamTask_HandleCloseWithProcessorError(t *testing.T) {
	s := new(streams.MockSource)
	s.On("Consume").Return(streams.NewMessage(nil, nil), nil)
	s.On("Close").Return(nil)

	p := new(streams.MockProcessor)
	p.On("WithPipe", mock.Anything)
	p.On("Close").Return(errors.New("test error"))

	b := streams.NewStreamBuilder()
	b.Source("src", s).
		Process("processor", p)

	tp, _ := b.Build()
	task := streams.NewTask(tp)
	_ = task.Start(context.Background())

	time.Sleep(time.Millisecond)

	err := task.Close()

	assert.Error(t, err)
}

func TestStreamTask_HandleCloseWithSourceError(t *testing.T) {
	s := new(streams.MockSource)
	s.On("Consume").Return(streams.NewMessage(nil, nil), nil)
	s.On("Close").Return(errors.New("test error"))

	b := streams.NewStreamBuilder()
	b.Source("src", s)

	tp, _ := b.Build()
	task := streams.NewTask(tp)
	_ = task.Start(context.Background())

	time.Sleep(time.Millisecond)

	err := task.Close()

	assert.Error(t, err)
}

func TestTasks_Start(t *testing.T) {
	ctx := context.Background()
	t1, t2, t3 := new(streams.MockTask), new(streams.MockTask), new(streams.MockTask)
	t1.On("Start", ctx).Return(nil)
	t2.On("Start", ctx).Return(nil)
	t3.On("Start", ctx).Return(nil)

	tasks := streams.Tasks{t1, t2, t3}

	err := tasks.Start(ctx)

	assert.NoError(t, err)
	t1.AssertExpectations(t)
	t2.AssertExpectations(t)
	t3.AssertExpectations(t)
	assert.True(t, t1.StartCalled.Before(t2.StartCalled))
	assert.True(t, t2.StartCalled.Before(t3.StartCalled))
}

func TestTasks_Start_WithError(t *testing.T) {
	ctx := context.Background()
	t1, t2, t3 := new(streams.MockTask), new(streams.MockTask), new(streams.MockTask)
	t1.On("Start", ctx).Return(nil)
	t2.On("Start", ctx).Return(errors.New("test error"))

	tasks := streams.Tasks{t1, t2, t3}

	err := tasks.Start(ctx)

	assert.Error(t, err)
	t1.AssertExpectations(t)
	t2.AssertExpectations(t)
	t3.AssertNotCalled(t, "Start")
}

func TestTasks_OnError(t *testing.T) {
	fn := streams.ErrorFunc(func(err error) error { return err })
	t1, t2, t3 := new(streams.MockTask), new(streams.MockTask), new(streams.MockTask)
	t1.On("OnError", mock.AnythingOfType("streams.ErrorFunc")).Return()
	t2.On("OnError", mock.AnythingOfType("streams.ErrorFunc")).Return()
	t3.On("OnError", mock.AnythingOfType("streams.ErrorFunc")).Return()

	tasks := streams.Tasks{t1, t2, t3}

	tasks.OnError(fn)

	t1.AssertExpectations(t)
	t2.AssertExpectations(t)
	t3.AssertExpectations(t)
	assert.True(t, t1.OnErrorCalled.Before(t2.OnErrorCalled))
	assert.True(t, t2.OnErrorCalled.Before(t3.OnErrorCalled))
}

func TestTasks_Close(t *testing.T) {
	t1, t2, t3 := new(streams.MockTask), new(streams.MockTask), new(streams.MockTask)
	t1.On("Close").Return(nil)
	t2.On("Close").Return(nil)
	t3.On("Close").Return(nil)

	tasks := streams.Tasks{t1, t2, t3}

	err := tasks.Close()

	assert.NoError(t, err)
	t1.AssertExpectations(t)
	t2.AssertExpectations(t)
	t3.AssertExpectations(t)
	assert.True(t, t1.CloseCalled.After(t2.CloseCalled))
	assert.True(t, t2.CloseCalled.After(t3.CloseCalled))
}

func TestTasks_Close_WithError(t *testing.T) {
	t1, t2, t3 := new(streams.MockTask), new(streams.MockTask), new(streams.MockTask)
	t2.On("Close").Return(errors.New("test error"))
	t3.On("Close").Return(nil)

	tasks := streams.Tasks{t1, t2, t3}

	err := tasks.Close()

	assert.Error(t, err)
	t1.AssertNotCalled(t, "Close")
	t2.AssertExpectations(t)
	t3.AssertExpectations(t)
}
