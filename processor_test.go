package streams_test

import (
	"testing"

	"github.com/msales/streams"
	"github.com/msales/streams/mocks"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestBranchProcessor_Process(t *testing.T) {
	truePred := func(msg *streams.Message) (bool, error) {
		return true, nil
	}
	falsePred := func(msg *streams.Message) (bool, error) {
		return false, nil
	}
	ctx := mocks.NewPipe(t)
	ctx.ExpectForwardToChild("test", "test", 0)
	p := streams.NewBranchProcessor([]streams.Predicate{truePred, falsePred})
	p.WithPipe(ctx)

	err := p.Process(streams.NewMessage("test", "test"))

	assert.NoError(t, err)
	ctx.AssertExpectations()
}

func TestBranchProcessor_ProcessWithError(t *testing.T) {
	errPred := func(msg *streams.Message) (bool, error) {
		return true, errors.New("test")
	}
	ctx := mocks.NewPipe(t)
	p := streams.NewBranchProcessor([]streams.Predicate{errPred})
	p.WithPipe(ctx)

	err := p.Process(streams.NewMessage("test", "test"))

	assert.Error(t, err)
}

func TestBranchProcessor_ProcessWithForwardError(t *testing.T) {
	pred := func(msg *streams.Message) (bool, error) {
		return true, nil
	}
	ctx := mocks.NewPipe(t)
	ctx.ExpectForwardToChild("test", "test", 0)
	ctx.ShouldError()
	p := streams.NewBranchProcessor([]streams.Predicate{pred})
	p.WithPipe(ctx)

	err := p.Process(streams.NewMessage("test", "test"))

	assert.Error(t, err)
}

func TestBranchProcessor_Close(t *testing.T) {
	p := streams.NewBranchProcessor([]streams.Predicate{})

	err := p.Close()

	assert.NoError(t, err)
}

func TestFilterProcessor_Process(t *testing.T) {
	pred := func(msg *streams.Message) (bool, error) {
		if _, ok := msg.Key.(string); ok {
			return true, nil
		}

		return false, nil
	}
	ctx := mocks.NewPipe(t)
	ctx.ExpectForward("test", "test")
	p := streams.NewFilterProcessor(pred)
	p.WithPipe(ctx)

	p.Process(streams.NewMessage(1, 1))
	p.Process(streams.NewMessage("test", "test"))

	ctx.AssertExpectations()
}

func TestFilterProcessor_ProcessWithError(t *testing.T) {
	errPred := func(msg *streams.Message) (bool, error) {
		return true, errors.New("test")
	}
	ctx := mocks.NewPipe(t)
	p := streams.NewFilterProcessor(errPred)
	p.WithPipe(ctx)

	err := p.Process(streams.NewMessage("test", "test"))

	assert.Error(t, err)
}

func TestFilterProcessor_Close(t *testing.T) {
	p := streams.NewFilterProcessor(nil)

	err := p.Close()

	assert.NoError(t, err)
}

func TestMapProcessor_Process(t *testing.T) {
	mapper := func(msg *streams.Message) (*streams.Message, error) {
		return streams.NewMessage(1, 1), nil
	}
	ctx := mocks.NewPipe(t)
	ctx.ExpectForward(1, 1)
	p := streams.NewMapProcessor(mapper)
	p.WithPipe(ctx)

	p.Process(streams.NewMessage("test", "test"))

	ctx.AssertExpectations()
}

func TestMapProcessor_ProcessWithError(t *testing.T) {
	mapper := func(msg *streams.Message) (*streams.Message, error) {
		return nil, errors.New("test")
	}
	ctx := mocks.NewPipe(t)
	p := streams.NewMapProcessor(mapper)
	p.WithPipe(ctx)

	err := p.Process(streams.NewMessage("test", "test"))

	assert.Error(t, err)
}

func TestMapProcessor_Close(t *testing.T) {
	p := streams.NewMapProcessor(nil)

	err := p.Close()

	assert.NoError(t, err)
}

func TestMergeProcessor_Process(t *testing.T) {
	ctx := mocks.NewPipe(t)
	ctx.ExpectForward("test", "test")
	p := streams.NewMergeProcessor()
	p.WithPipe(ctx)

	p.Process(streams.NewMessage("test", "test"))

	ctx.AssertExpectations()
}

func TestMergeProcessor_Close(t *testing.T) {
	p := streams.NewMergeProcessor()

	err := p.Close()

	assert.NoError(t, err)
}

func TestPrintProcessor_Process(t *testing.T) {
	ctx := mocks.NewPipe(t)
	ctx.ExpectForward("test", "test")
	p := streams.NewPrintProcessor()
	p.WithPipe(ctx)

	p.Process(streams.NewMessage("test", "test"))

	ctx.AssertExpectations()
}

func TestPrintProcessor_Close(t *testing.T) {
	p := streams.NewPrintProcessor()

	err := p.Close()

	assert.NoError(t, err)
}
