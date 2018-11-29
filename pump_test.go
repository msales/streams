package streams_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/msales/pkg/v3/stats"
	"github.com/msales/streams"
	"github.com/stretchr/testify/assert"
)

func TestProcessorPump_Accept(t *testing.T) {
	ctx := stats.WithStats(context.Background(), stats.Null)
	msg := streams.NewMessageWithContext(ctx, "test", "test")
	processor := new(MockProcessor)
	processor.On("Process", msg).Return(nil)
	processor.On("Close").Maybe().Return(nil)
	node := streams.NewProcessorNode("test", processor)
	pipe := new(MockTimedPipe)
	pipe.On("Reset")
	pipe.On("Duration").Return(time.Duration(0))
	p := streams.NewPump(node, pipe, func(error) {})
	defer p.Close()

	err := p.Accept(msg)

	time.Sleep(time.Millisecond)

	assert.NoError(t, err)
	processor.AssertExpectations(t)
}

func TestProcessorPump_AcceptError(t *testing.T) {
	var err error

	msg := streams.NewMessage("test", "test")
	processor := new(MockProcessor)
	processor.On("Process", msg).Return(errors.New("test"))
	processor.On("Close").Return(nil)
	node := streams.NewProcessorNode("test", processor)
	pipe := new(MockTimedPipe)
	pipe.On("Reset")
	pipe.On("Duration").Return(time.Duration(0))
	p := streams.NewPump(node, pipe, func(e error) {
		err = e
	})
	defer p.Close()

	p.Accept(msg)

	time.Sleep(time.Millisecond)

	assert.Error(t, err)
}

func TestProcessorPump_WithLock(t *testing.T) {
	processor := new(MockProcessor)
	node := streams.NewProcessorNode("test", processor)
	p := streams.NewPump(node, nil, func(error) {})

	err := p.WithLock(func() error {
		return errors.New("test")
	})

	assert.Error(t, err)
}

func TestProcessorPump_Close(t *testing.T) {
	processor := new(MockProcessor)
	processor.On("Close").Return(nil)
	node := streams.NewProcessorNode("test", processor)
	pipe := new(MockTimedPipe)
	p := streams.NewPump(node, pipe, func(error) {})

	err := p.Close()

	assert.NoError(t, err)
	processor.AssertExpectations(t)
}

func TestProcessorPump_CloseError(t *testing.T) {
	processor := new(MockProcessor)
	processor.On("Close").Return(errors.New("test"))
	node := streams.NewProcessorNode("test", processor)
	pipe := new(MockTimedPipe)
	p := streams.NewPump(node, pipe, func(error) {})

	err := p.Close()

	assert.Error(t, err)
}

func TestSourcePump_CanConsume(t *testing.T) {
	ctx := stats.WithStats(context.Background(), stats.Null)
	msg := streams.NewMessageWithContext(ctx, "test", "test")
	source := new(MockSource)
	source.On("Consume").Maybe().Return(msg, nil)
	source.On("Close").Return(nil)
	pump := new(MockPump)
	pump.On("Accept", msg).Return(nil)
	p := streams.NewSourcePump("test", source, []streams.Pump{pump}, func(error) {})
	defer p.Close()
	defer p.Stop()

	time.Sleep(time.Millisecond)

	pump.AssertExpectations(t)
}

func TestSourcePump_HandlesPumpError(t *testing.T) {
	gotError := false
	msg := streams.NewMessage("test", "test")
	source := new(MockSource)
	source.On("Consume").Maybe().Return(msg, nil)
	source.On("Close").Return(nil)
	pump := new(MockPump)
	pump.On("Accept", msg).Return(errors.New("test"))
	p := streams.NewSourcePump("test", source, []streams.Pump{pump}, func(error) {
		gotError = true
	})
	defer p.Close()
	defer p.Stop()

	time.Sleep(time.Millisecond)

	assert.True(t, gotError)
}

func TestSourcePump_Close(t *testing.T) {
	source := new(MockSource)
	source.On("Consume").Maybe().Return(streams.NewMessage("test", "test"), nil)
	source.On("Close").Return(nil)
	p := streams.NewSourcePump("test", source, []streams.Pump{}, func(error) {})
	p.Stop()

	err := p.Close()

	assert.NoError(t, err)
	source.AssertExpectations(t)
}

func TestSourcePump_CloseError(t *testing.T) {
	source := new(MockSource)
	source.On("Consume").Return(streams.NewMessage("test", "test"), nil)
	source.On("Close").Return(errors.New("test"))
	p := streams.NewSourcePump("test", source, []streams.Pump{}, func(error) {})
	p.Stop()

	err := p.Close()

	assert.Error(t, err)
}
