package mocks

import (
	"testing"

	"github.com/msales/streams"
)

func TestPipe_ImplementsPipeInterface(t *testing.T) {
	var c interface{} = &Pipe{}

	if _, ok := c.(streams.Pipe); !ok {
		t.Error("The mock Pipe should implement the streams.Pipe interface.")
	}
}

func TestPipe_HandlesExpectations(t *testing.T) {
	c := NewPipe(t)

	c.ExpectForward("test", "test")
	c.ExpectForwardToChild("test", "test", 1)
	c.ExpectCommit()

	c.Forward(streams.NewMessage("test", "test"))
	c.ForwardToChild(streams.NewMessage("test", "test"), 1)
	c.Commit()
	c.AssertExpectations()
}

func TestPipe_WithoutExpectationOnForward(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when no expectation on Forward")
		}

	}()
	c := NewPipe(mockT)

	c.Forward(streams.NewMessage("test", "test"))
}

func TestPipe_WithWrongExpectationOnForward(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when wrong expectation on Forward")
		}

	}()
	c := NewPipe(mockT)
	c.ExpectForward(1, 1)

	c.Forward(streams.NewMessage("test", "test"))
}

func TestPipe_WithShouldErrorOnForward(t *testing.T) {
	mockT := new(testing.T)
	c := NewPipe(mockT)
	c.ExpectForward("test", "test")
	c.ShouldError()

	err := c.Forward(streams.NewMessage("test", "test"))

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
	c := NewPipe(mockT)

	c.ForwardToChild(streams.NewMessage("test", "test"), 1)
}

func TestPipeWithWrongExpectationOnForwardToChild(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when wrong expectation on ForwardToChild")
		}

	}()
	c := NewPipe(mockT)
	c.ExpectForwardToChild(1, 1, 3)

	c.ForwardToChild(streams.NewMessage("test", "test"), 1)
}

func TestPipe_WithShouldErrorOnForwardToChild(t *testing.T) {
	mockT := new(testing.T)
	c := NewPipe(mockT)
	c.ExpectForwardToChild("test", "test", 1)
	c.ShouldError()

	err := c.ForwardToChild(streams.NewMessage("test", "test"), 1)

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
	c := NewPipe(mockT)

	c.Commit()
}

func TestPipe_WithErrorOnCommit(t *testing.T) {
	mockT := new(testing.T)
	c := NewPipe(mockT)
	c.ExpectCommit()
	c.ShouldError()

	err := c.Commit()

	if err == nil {
		t.Error("Expected error but got none")
	}
}

func TestPipe_WithUnfulfilledExpectationOnForward(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when unforfilled expectation on Forward")
		}

	}()
	c := NewPipe(mockT)
	c.ExpectForward(1, 1)

	c.AssertExpectations()
}

func TestPipe_WithUnfulfilledExpectationOnForwardToChild(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when unforfilled expectation on ForwardToChild")
		}

	}()
	c := NewPipe(mockT)
	c.ExpectForwardToChild(1, 1, 1)

	c.AssertExpectations()
}

func TestPipe_WithUnfulfilledExpectationOnCommit(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when unforfilled expectation on Commit")
		}

	}()
	c := NewPipe(mockT)
	c.ExpectCommit()

	c.AssertExpectations()
}
