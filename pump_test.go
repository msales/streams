package streams_test

import (
	"errors"
	"github.com/msales/streams/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
)

func TestSyncPump_Accept(t *testing.T) {
	mon := new(streams.MockMonitor)
	mon.On("Processed", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	msg := streams.NewMessage("test", "test")
	processor := new(streams.MockProcessor)
	processor.On("Process", msg).Return(nil)
	processor.On("Close").Maybe().Return(nil)
	node := streams.NewProcessorNode("test", processor)
	pipe := new(streams.MockTimedPipe)
	pipe.On("Reset")
	pipe.On("Duration").Return(time.Duration(0))
	p := streams.NewSyncPump(mon, node, pipe, func(err error) error { return err })
	defer p.Close()

	err := p.Accept(msg)

	assert.NoError(t, err)
	processor.AssertExpectations(t)
	mon.AssertExpectations(t)
}

func TestSyncPump_AcceptError(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	processor := new(streams.MockProcessor)
	processor.On("Process", msg).Return(errors.New("test"))
	processor.On("Close").Return(nil)
	node := streams.NewProcessorNode("test", processor)
	pipe := new(streams.MockTimedPipe)
	pipe.On("Reset")
	pipe.On("Duration").Return(time.Duration(0))
	p := streams.NewSyncPump(&fakeMonitor{}, node, pipe, func(err error) error { return err })
	defer p.Close()

	err := p.Accept(msg)

	assert.Error(t, err)
}

func TestSyncPump_AcceptError_ErrorIsNotReturned(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	processor := new(streams.MockProcessor)
	processor.On("Process", msg).Return(errors.New("test"))
	processor.On("Close").Return(nil)
	node := streams.NewProcessorNode("test", processor)
	pipe := new(streams.MockTimedPipe)
	pipe.On("Reset")
	pipe.On("Duration").Return(time.Duration(0))
	p := streams.NewSyncPump(&fakeMonitor{}, node, pipe, func(err error) error { return nil })
	defer p.Close()

	err := p.Accept(msg)

	assert.NoError(t, err)
}

func TestSyncPump_Close(t *testing.T) {
	processor := new(streams.MockProcessor)
	processor.On("Close").Return(nil)
	node := streams.NewProcessorNode("test", processor)
	pipe := new(streams.MockTimedPipe)
	p := streams.NewSyncPump(&fakeMonitor{}, node, pipe, func(err error) error { return err })

	err := p.Close()

	assert.NoError(t, err)
	processor.AssertExpectations(t)
}

func TestSyncPump_CloseError(t *testing.T) {
	processor := new(streams.MockProcessor)
	processor.On("Close").Return(errors.New("test"))
	node := streams.NewProcessorNode("test", processor)
	pipe := new(streams.MockTimedPipe)
	p := streams.NewSyncPump(&fakeMonitor{}, node, pipe, func(err error) error { return err })

	err := p.Close()

	assert.Error(t, err)
}

func TestSyncPump_CloseError_ErrorIsNotReturned(t *testing.T) {
	processor := new(streams.MockProcessor)
	processor.On("Close").Return(errors.New("test"))
	node := streams.NewProcessorNode("test", processor)
	pipe := new(streams.MockTimedPipe)
	p := streams.NewSyncPump(&fakeMonitor{}, node, pipe, func(err error) error { return nil })

	err := p.Close()

	assert.Error(t, err)
}

func TestAsyncPump_Accept(t *testing.T) {
	mon := new(streams.MockMonitor)
	mon.On("Processed", mock.Anything, mock.Anything, mock.Anything).Maybe().Return(nil)
	msg := streams.NewMessage("test", "test")
	processor := new(streams.MockProcessor)
	processor.On("Process", msg).Return(nil)
	processor.On("Close").Maybe().Return(nil)
	node := streams.NewProcessorNode("test", processor)
	pipe := new(streams.MockTimedPipe)
	pipe.On("Reset")
	pipe.On("Duration").Return(time.Duration(0))
	p := streams.NewAsyncPump(mon, node, pipe, func(err error) error { return err })
	defer p.Close()

	err := p.Accept(msg)

	time.Sleep(3 * time.Millisecond)

	assert.NoError(t, err)
	processor.AssertExpectations(t)
	mon.AssertExpectations(t)
}

func TestAsyncPump_AcceptError(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	processor := new(streams.MockProcessor)
	processor.On("Process", msg).Return(errors.New("test"))
	processor.On("Close").Return(nil)
	node := streams.NewProcessorNode("test", processor)
	pipe := new(streams.MockTimedPipe)
	pipe.On("Reset")
	pipe.On("Duration").Return(time.Duration(0))
	p := streams.NewAsyncPump(&fakeMonitor{}, node, pipe, func(e error) error {
		assert.Error(t, e)
		return e
	})
	defer p.Close()

	p.Accept(msg)

	time.Sleep(time.Millisecond)
}

func TestAsyncPump_AcceptError_ErrorIsNotReturned(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	processor := new(streams.MockProcessor)
	processor.On("Process", msg).Return(errors.New("test"))
	processor.On("Close").Return(nil)
	node := streams.NewProcessorNode("test", processor)
	pipe := new(streams.MockTimedPipe)
	pipe.On("Reset")
	pipe.On("Duration").Return(time.Duration(0))
	p := streams.NewAsyncPump(&fakeMonitor{}, node, pipe, func(e error) error {
		assert.Error(t, e)
		return nil
	})
	defer p.Close()

	p.Accept(msg)

	time.Sleep(time.Millisecond)
}

func TestAsyncPump_Close(t *testing.T) {
	processor := new(streams.MockProcessor)
	processor.On("Close").Return(nil)
	node := streams.NewProcessorNode("test", processor)
	pipe := new(streams.MockTimedPipe)
	p := streams.NewAsyncPump(&fakeMonitor{}, node, pipe, func(err error) error { return err })

	err := p.Close()

	assert.NoError(t, err)
	processor.AssertExpectations(t)
}

func TestAsyncPump_CloseError(t *testing.T) {
	processor := new(streams.MockProcessor)
	processor.On("Close").Return(errors.New("test"))
	node := streams.NewProcessorNode("test", processor)
	pipe := new(streams.MockTimedPipe)
	p := streams.NewAsyncPump(&fakeMonitor{}, node, pipe, func(err error) error { return err })

	err := p.Close()

	assert.Error(t, err)
}

func TestNewSourcePump(t *testing.T) {
	source := new(streams.MockSource)
	source.On("Close").Return(nil)
	pump := new(streams.MockPump)

	p := streams.NewSourcePump(&fakeMonitor{}, "test", source, []streams.Pump{pump}, func(err error) error { return err })

	assert.Implements(t, (*streams.SourcePump)(nil), p)

	p.Close()
}

func TestSourcePump_CanConsume(t *testing.T) {
	mon := new(streams.MockMonitor)
	mon.On("Processed", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	msg := streams.NewMessage("test", "test")
	source := new(streams.MockSource)
	source.On("Consume").Maybe().Return(msg, nil)
	source.On("Close").Return(nil)
	pump := new(streams.MockPump)
	pump.On("Accept", msg).Return(nil)
	p := streams.NewSourcePump(mon, "test", source, []streams.Pump{pump}, func(err error) error { return err })
	defer p.Close()
	defer p.Stop()

	time.Sleep(time.Millisecond)

	pump.AssertExpectations(t)
	mon.AssertExpectations(t)
}

func TestSourcePump_HandlesPumpError(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	source := new(streams.MockSource)
	source.On("Consume").Maybe().Return(msg, nil)
	source.On("Close").Return(nil)
	pump := new(streams.MockPump)
	pump.On("Accept", msg).Return(errors.New("test"))
	p := streams.NewSourcePump(&fakeMonitor{}, "test", source, []streams.Pump{pump}, func(e error) error {
		assert.Error(t, e)
		return e
	})
	defer p.Close()
	defer p.Stop()

	time.Sleep(time.Millisecond)
}

func TestSourcePump_HandlesPumpError_ErrorIsNotReturned(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	source := new(streams.MockSource)
	source.On("Consume").Maybe().Return(msg, nil)
	source.On("Close").Return(nil)
	pump := new(streams.MockPump)
	pump.On("Accept", msg).Return(errors.New("test"))
	p := streams.NewSourcePump(&fakeMonitor{}, "test", source, []streams.Pump{pump}, func(e error) error {
		assert.Error(t, e)
		return nil
	})
	defer p.Close()
	defer p.Stop()

	time.Sleep(time.Millisecond)
}

func TestSourcePump_Close(t *testing.T) {
	source := new(streams.MockSource)
	source.On("Consume").Maybe().Return(streams.NewMessage("test", "test"), nil)
	source.On("Close").Return(nil)
	p := streams.NewSourcePump(&fakeMonitor{}, "test", source, []streams.Pump{}, func(err error) error { return err })
	p.Stop()

	err := p.Close()

	assert.NoError(t, err)
	source.AssertExpectations(t)
}

func TestSourcePump_CloseError(t *testing.T) {
	source := new(streams.MockSource)
	source.On("Consume").Return(streams.NewMessage("test", "test"), nil)
	source.On("Close").Return(errors.New("test"))
	p := streams.NewSourcePump(&fakeMonitor{}, "test", source, []streams.Pump{}, func(err error) error { return err })
	p.Stop()

	err := p.Close()

	assert.Error(t, err)
}
