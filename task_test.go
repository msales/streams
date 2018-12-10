package streams_test

import (
	"errors"
	"testing"
	"time"

	"github.com/msales/streams/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func passThroughMapper(msg *streams.Message) (*streams.Message, error) {
	return msg, nil
}

func TestNewTask(t *testing.T) {
	task := streams.NewTask(nil)

	assert.Implements(t, (*streams.Task)(nil), task)
}

func TestStreamTask_ConsumesMessages(t *testing.T) {
	msgs := make(chan *streams.Message)
	msg := streams.NewMessage("test", "test")

	p := new(MockProcessor)
	p.On("WithPipe", mock.Anything).Return(nil)
	p.On("Process", msg).Return(nil)
	p.On("Close").Return(nil)

	b := streams.NewStreamBuilder()
	b.Source("src", &chanSource{msgs: msgs}).
		Map("pass-through", streams.MapperFunc(passThroughMapper)).
		Process("processor", p)

	tp, _ := b.Build()
	task := streams.NewTask(tp)
	task.OnError(func(err error) {
		t.FailNow()
	})

	err := task.Start()
	if err != nil {
		assert.FailNow(t, err.Error())
	}

	msgs <- msg

	time.Sleep(time.Millisecond)

	_ = task.Close()

	p.AssertExpectations(t)
}

func TestStreamTask_Throughput(t *testing.T) {
	msgs := make(chan *streams.Message)
	msg := streams.NewMessage("test", "test")

	count := 0

	b := streams.NewStreamBuilder()
	b.Source("src", &chanSource{msgs: msgs}).
		Map("pass-through", streams.MapperFunc(passThroughMapper)).
		Map("count", streams.MapperFunc(func(msg *streams.Message) (*streams.Message, error) {
			count++
			return msg, nil
		}))

	tp, _ := b.Build()
	task := streams.NewTask(tp)
	task.OnError(func(err error) {
		t.FailNow()
	})

	err := task.Start()
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
	msgs := make(chan *streams.Message)

	b := streams.NewStreamBuilder()
	b.Source("src", &chanSource{msgs: msgs})

	tp, _ := b.Build()
	task := streams.NewTask(tp)
	task.OnError(func(err error) {
		t.FailNow()
	})

	_ = task.Start()

	err := task.Start()

	_ = task.Close()

	assert.Error(t, err)
}

func TestStreamTask_HandleSourceError(t *testing.T) {
	gotError := false

	s := new(MockSource)
	s.On("Consume").Return(streams.NewMessage(nil, nil), errors.New("test error"))
	s.On("Close").Return(nil)

	b := streams.NewStreamBuilder()
	b.Source("src", s)

	tp, _ := b.Build()
	task := streams.NewTask(tp)
	task.OnError(func(err error) {
		gotError = true
	})

	_ = task.Start()

	time.Sleep(time.Millisecond)

	_ = task.Close()

	assert.True(t, gotError)
}

func TestStreamTask_HandleProcessorError(t *testing.T) {
	gotError := false

	msgs := make(chan *streams.Message)
	msg := streams.NewMessage("test", "test")

	p := new(MockProcessor)
	p.On("WithPipe", mock.Anything).Return(nil)
	p.On("Process", msg).Return(errors.New("test error"))
	p.On("Close").Return(nil)

	b := streams.NewStreamBuilder()
	b.Source("src", &chanSource{msgs: msgs}).
		Process("processor", p)

	tp, _ := b.Build()
	task := streams.NewTask(tp)
	task.OnError(func(err error) {
		gotError = true
	})

	_ = task.Start()

	msgs <- msg

	time.Sleep(time.Millisecond)

	_ = task.Close()

	assert.True(t, gotError)
}

func TestStreamTask_HandleCloseWithProcessorError(t *testing.T) {
	s := new(MockSource)
	s.On("Consume").Return(streams.NewMessage(nil, nil), nil)
	s.On("Close").Return(nil)

	p := new(MockProcessor)
	p.On("WithPipe", mock.Anything)
	p.On("Close").Return(errors.New("test error"))

	b := streams.NewStreamBuilder()
	b.Source("src", s).
		Process("processor", p)

	tp, _ := b.Build()
	task := streams.NewTask(tp)
	_ = task.Start()

	time.Sleep(time.Millisecond)

	err := task.Close()

	assert.Error(t, err)
}

func TestStreamTask_HandleCloseWithSourceError(t *testing.T) {
	s := new(MockSource)
	s.On("Consume").Return(streams.NewMessage(nil, nil), nil)
	s.On("Close").Return(errors.New("test error"))

	b := streams.NewStreamBuilder()
	b.Source("src", s)

	tp, _ := b.Build()
	task := streams.NewTask(tp)
	_ = task.Start()

	time.Sleep(time.Millisecond)

	err := task.Close()

	assert.Error(t, err)
}

type chanSource struct {
	msgs chan *streams.Message
}

func (s *chanSource) Consume() (*streams.Message, error) {
	select {

	case msg := <-s.msgs:
		return msg, nil

	case <-time.After(time.Millisecond):
		return streams.NewMessage(nil, nil), nil
	}
}

func (s *chanSource) Commit(v interface{}) error {
	return nil
}

func (s *chanSource) Close() error {
	close(s.msgs)

	return nil
}
