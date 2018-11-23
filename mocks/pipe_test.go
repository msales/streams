package mocks_test

import (
	"testing"

	"github.com/msales/streams"
	"github.com/msales/streams/mocks"
	"github.com/stretchr/testify/assert"
)

func TestPipe_ImplementsPipeInterface(t *testing.T) {
	var c interface{} = &mocks.Pipe{}

	if _, ok := c.(streams.Pipe); !ok {
		t.Error("The mock Pipe should implement the streams.Pipe interface.")
	}
}

func TestPipe_MessagesForForward(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	p := mocks.NewPipe(t)
	p.ExpectForward("test", "test")

	p.Forward(msg)

	msgs := p.Messages()
	assert.Len(t, msgs, 1)
	assert.Exactly(t, -1, msgs[0].Index)
	assert.Exactly(t, msg, msgs[0].Msg)
}

func TestPipe_MessagesForForwardToChild(t *testing.T) {
	msg := streams.NewMessage("test", "test")
	p := mocks.NewPipe(t)
	p.ExpectForwardToChild("test", "test", 0)

	p.ForwardToChild(msg, 0)

	msgs := p.Messages()
	assert.Len(t, msgs, 1)
	assert.Exactly(t, 0, msgs[0].Index)
	assert.Exactly(t, msg, msgs[0].Msg)
}

func TestPipe_HandlesExpectations(t *testing.T) {
	p := mocks.NewPipe(t)

	p.ExpectMark("test", "test")
	p.ExpectForward("test", "test")
	p.ExpectForwardToChild("test", "test", 1)
	p.ExpectCommit()

	p.Mark(streams.NewMessage("test", "test"))
	p.Forward(streams.NewMessage("test", "test"))
	p.ForwardToChild(streams.NewMessage("test", "test"), 1)
	p.Commit(streams.NewMessage("test", "test"))
	p.AssertExpectations()
}

func TestPipe_HandlesAnythingExpectations(t *testing.T) {
	p := mocks.NewPipe(t)

	p.ExpectMark(mocks.Anything, mocks.Anything)
	p.ExpectForward(mocks.Anything, mocks.Anything)
	p.ExpectForwardToChild(mocks.Anything, mocks.Anything, 1)

	p.Mark(streams.NewMessage("test", "test"))
	p.Forward(streams.NewMessage("test", "test"))
	p.ForwardToChild(streams.NewMessage("test", "test"), 1)
	p.AssertExpectations()
}

func TestPipe_WithoutExpectationOnMark(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when no expectation on Mark")
		}

	}()
	p := mocks.NewPipe(mockT)

	p.Mark(streams.NewMessage("test", "test"))
}

func TestPipe_WithWrongExpectationOnMark(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when wrong expectation on Mark")
		}

	}()
	p := mocks.NewPipe(mockT)
	p.ExpectMark(1, 1)

	p.Mark(streams.NewMessage("test", "test"))
}

func TestPipe_WithShouldErrorOnMark(t *testing.T) {
	mockT := new(testing.T)
	p := mocks.NewPipe(mockT)
	p.ExpectMark("test", "test")
	p.ShouldError()

	err := p.Mark(streams.NewMessage("test", "test"))

	if err == nil {
		t.Error("Expected error but got none")
	}
}

func TestPipe_WithoutExpectationOnForward(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when no expectation on Forward")
		}

	}()
	p := mocks.NewPipe(mockT)

	p.Forward(streams.NewMessage("test", "test"))
}

func TestPipe_WithWrongExpectationOnForward(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when wrong expectation on Forward")
		}

	}()
	p := mocks.NewPipe(mockT)
	p.ExpectForward(1, 1)

	p.Forward(streams.NewMessage("test", "test"))
}

func TestPipe_WithShouldErrorOnForward(t *testing.T) {
	mockT := new(testing.T)
	p := mocks.NewPipe(mockT)
	p.ExpectForward("test", "test")
	p.ShouldError()

	err := p.Forward(streams.NewMessage("test", "test"))

	if err == nil {
		t.Error("Expected error but got none")
	}
}

func TestPipe_WithoutExpectationOnForwardToChild(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when no expectation on Forward")
		}

	}()
	c := mocks.NewPipe(mockT)

	c.ForwardToChild(streams.NewMessage("test", "test"), 1)
}

func TestPipeWithWrongExpectationOnForwardToChild(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when wrong expectation on ForwardToChild")
		}

	}()
	p := mocks.NewPipe(mockT)
	p.ExpectForwardToChild(1, 1, 3)

	p.ForwardToChild(streams.NewMessage("test", "test"), 1)
}

func TestPipe_WithShouldErrorOnForwardToChild(t *testing.T) {
	mockT := new(testing.T)
	p := mocks.NewPipe(mockT)
	p.ExpectForwardToChild("test", "test", 1)
	p.ShouldError()

	err := p.ForwardToChild(streams.NewMessage("test", "test"), 1)

	if err == nil {
		t.Error("Expected error but got none")
	}
}

func TestPipe_WithoutExpectationOnCommit(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when no expectation on Commit")
		}

	}()
	p := mocks.NewPipe(mockT)

	p.Commit(streams.NewMessage("test", "test"))
}

func TestPipe_WithErrorOnCommit(t *testing.T) {
	mockT := new(testing.T)
	p := mocks.NewPipe(mockT)
	p.ExpectCommit()
	p.ShouldError()

	err := p.Commit(streams.NewMessage("test", "test"))

	if err == nil {
		t.Error("Expected error but got none")
	}
}

func TestPipe_WithUnfulfilledExpectationOnMark(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when unfulfilled expectation on Mark")
		}

	}()
	p := mocks.NewPipe(mockT)
	p.ExpectMark(1, 1)

	p.AssertExpectations()
}

func TestPipe_WithUnfulfilledExpectationOnForward(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when unfulfilled expectation on Forward")
		}

	}()
	p := mocks.NewPipe(mockT)
	p.ExpectForward(1, 1)

	p.AssertExpectations()
}

func TestPipe_WithUnfulfilledExpectationOnForwardToChild(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when unfulfilled expectation on ForwardToChild")
		}

	}()
	p := mocks.NewPipe(mockT)
	p.ExpectForwardToChild(1, 1, 1)

	p.AssertExpectations()
}

func TestPipe_WithUnfulfilledExpectationOnCommit(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when unfulfilled expectation on Commit")
		}

	}()
	p := mocks.NewPipe(mockT)
	p.ExpectCommit()

	p.AssertExpectations()
}
