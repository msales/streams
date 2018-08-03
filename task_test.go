package streams_test

import (
	"errors"
	"testing"
	"time"

	"github.com/msales/streams"
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
		Map("pass-through", passThroughMapper).
		Process("processor", p)

	task := streams.NewTask(b.Build())
	task.OnError(func(err error) {
		t.FailNow()
	})

	task.Start()

	msgs <- msg

	time.Sleep(time.Millisecond)

	task.Close()

	p.AssertExpectations(t)
}

func TestStreamTask_CannotStartTwice(t *testing.T) {
	msgs := make(chan *streams.Message)

	b := streams.NewStreamBuilder()
	b.Source("src", &chanSource{msgs: msgs})

	task := streams.NewTask(b.Build())
	task.OnError(func(err error) {
		t.FailNow()
	})

	task.Start()

	err := task.Start()

	task.Close()

	assert.Error(t, err)
}

func TestStreamTask_HandleSourceError(t *testing.T) {
	gotError := false

	s := new(MockSource)
	s.On("Consume").Return(streams.NewMessage(nil, nil), errors.New("test error"))
	s.On("Close").Return(nil)

	b := streams.NewStreamBuilder()
	b.Source("src", s)

	task := streams.NewTask(b.Build())
	task.OnError(func(err error) {
		gotError = true
	})

	task.Start()

	time.Sleep(time.Millisecond)

	task.Close()

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

	task := streams.NewTask(b.Build())
	task.OnError(func(err error) {
		gotError = true
	})

	task.Start()

	msgs <- msg

	time.Sleep(time.Millisecond)

	task.Close()

	assert.True(t, gotError)
}

func TestStreamTask_HandleCloseWithProcessorError(t *testing.T) {
	s := new(MockSource)
	s.On("Consume").Return(streams.NewMessage(nil, nil), nil)
	s.On("Close").Return(nil)

	p := new(MockProcessor)
	p.On("WithPipe", mock.Anything).Return(nil)
	p.On("Close").Return(errors.New("test error"))

	b := streams.NewStreamBuilder()
	b.Source("src", s).
		Process("processor", p)

	task := streams.NewTask(b.Build())
	task.Start()

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

	task := streams.NewTask(b.Build())
	task.Start()

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
