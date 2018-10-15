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
	pipe := mocks.NewPipe(t)
	pipe.ExpectForwardToChild("test", "test", 0)
	p := streams.NewBranchProcessor([]streams.Predicate{truePred, falsePred})
	p.WithPipe(pipe)

	err := p.Process(streams.NewMessage("test", "test"))

	assert.NoError(t, err)
	pipe.AssertExpectations()
}

func TestBranchProcessor_ProcessWithError(t *testing.T) {
	errPred := func(msg *streams.Message) (bool, error) {
		return true, errors.New("test")
	}
	pipe := mocks.NewPipe(t)
	p := streams.NewBranchProcessor([]streams.Predicate{errPred})
	p.WithPipe(pipe)

	err := p.Process(streams.NewMessage("test", "test"))

	assert.Error(t, err)
}

func TestBranchProcessor_ProcessWithForwardError(t *testing.T) {
	pred := func(msg *streams.Message) (bool, error) {
		return true, nil
	}
	pipe := mocks.NewPipe(t)
	pipe.ExpectForwardToChild("test", "test", 0)
	pipe.ShouldError()
	p := streams.NewBranchProcessor([]streams.Predicate{pred})
	p.WithPipe(pipe)

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
	pipe := mocks.NewPipe(t)
	pipe.ExpectForward("test", "test")
	p := streams.NewFilterProcessor(pred)
	p.WithPipe(pipe)

	p.Process(streams.NewMessage(1, 1))
	p.Process(streams.NewMessage("test", "test"))

	pipe.AssertExpectations()
}

func TestFilterProcessor_ProcessWithError(t *testing.T) {
	errPred := func(msg *streams.Message) (bool, error) {
		return true, errors.New("test")
	}
	pipe := mocks.NewPipe(t)
	p := streams.NewFilterProcessor(errPred)
	p.WithPipe(pipe)

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
	pipe := mocks.NewPipe(t)
	pipe.ExpectForward(1, 1)
	p := streams.NewMapProcessor(mapper)
	p.WithPipe(pipe)

	p.Process(streams.NewMessage("test", "test"))

	pipe.AssertExpectations()
}

func TestMapProcessor_ProcessWithError(t *testing.T) {
	mapper := func(msg *streams.Message) (*streams.Message, error) {
		return nil, errors.New("test")
	}
	pipe := mocks.NewPipe(t)
	p := streams.NewMapProcessor(mapper)
	p.WithPipe(pipe)

	err := p.Process(streams.NewMessage("test", "test"))

	assert.Error(t, err)
}

func TestMapProcessor_Close(t *testing.T) {
	p := streams.NewMapProcessor(nil)

	err := p.Close()

	assert.NoError(t, err)
}

func TestFlatMapProcessor_Process(t *testing.T) {
	mapper := func(msg *streams.Message) ([]*streams.Message, error) {
		return []*streams.Message{
			streams.NewMessage(1, 1),
			streams.NewMessage(2, 2),
		}, nil
	}
	pipe := mocks.NewPipe(t)
	pipe.ExpectForward(1, 1)
	pipe.ExpectForward(2, 2)
	p := streams.NewFlatMapProcessor(mapper)
	p.WithPipe(pipe)

	p.Process(streams.NewMessage("test", "test"))

	pipe.AssertExpectations()
}

func TestFlatMapProcessor_ProcessWithError(t *testing.T) {
	mapper := func(msg *streams.Message) ([]*streams.Message, error) {
		return nil, errors.New("test")
	}
	pipe := mocks.NewPipe(t)
	p := streams.NewFlatMapProcessor(mapper)
	p.WithPipe(pipe)

	err := p.Process(streams.NewMessage("test", "test"))

	assert.Error(t, err)
}

func TestFlatMapProcessor_ProcessWithForawrdError(t *testing.T) {
	mapper := func(msg *streams.Message) ([]*streams.Message, error) {
		return []*streams.Message{
			streams.NewMessage(1, 1),
			streams.NewMessage(2, 2),
		}, nil
	}
	pipe := mocks.NewPipe(t)
	pipe.ExpectForward(1, 1)
	pipe.ShouldError()
	p := streams.NewFlatMapProcessor(mapper)
	p.WithPipe(pipe)

	err := p.Process(streams.NewMessage("test", "test"))

	assert.Error(t, err)
	pipe.AssertExpectations()
}

func TestFlatMapProcessor_Close(t *testing.T) {
	p := streams.NewFlatMapProcessor(nil)

	err := p.Close()

	assert.NoError(t, err)
}

func TestMergeProcessor_Process(t *testing.T) {
	pipe := mocks.NewPipe(t)
	pipe.ExpectForward("test", "test")
	p := streams.NewMergeProcessor()
	p.WithPipe(pipe)

	p.Process(streams.NewMessage("test", "test"))

	pipe.AssertExpectations()
}

func TestMergeProcessor_ProcessMergesMetadata(t *testing.T) {
	src1 := new(MockSource)
	src2 := new(MockSource)
	pipe := mocks.NewPipe(t)
	pipe.ExpectForward(nil, "test")
	pipe.ExpectForward("test", "test")

	p := streams.NewMergeProcessor()
	p.WithPipe(pipe)

	p.Process(streams.NewMessage(nil, "test").WithMetadata(src1, "test1"))
	p.Process(streams.NewMessage("test", "test").WithMetadata(src2, "test2"))

	queue := pipe.Messages()
	assert.Len(t, queue, 2)
	msg := queue[1].Msg
	assert.Len(t, msg.Metadata(), 2)
	assert.Equal(t, "test1", msg.Metadata()[src1])
	assert.Equal(t, "test2", msg.Metadata()[src2])

	pipe.AssertExpectations()
}

func TestMergeProcessor_Close(t *testing.T) {
	p := streams.NewMergeProcessor()

	err := p.Close()

	assert.NoError(t, err)
}

func TestPrintProcessor_Process(t *testing.T) {
	pipe := mocks.NewPipe(t)
	pipe.ExpectForward("test", "test")
	p := streams.NewPrintProcessor()
	p.WithPipe(pipe)

	p.Process(streams.NewMessage("test", "test"))

	pipe.AssertExpectations()
}

func TestPrintProcessor_Close(t *testing.T) {
	p := streams.NewPrintProcessor()

	err := p.Close()

	assert.NoError(t, err)
}
