package mocks

import (
	"testing"

	"github.com/msales/streams"
	"github.com/stretchr/testify/assert"
)

func TestPipe_ImplementsPipeInterface(t *testing.T) {
	var c interface{} = &Pipe{}

	if _, ok := c.(streams.Pipe); !ok {
		t.Error("The mock Pipe should implement the streams.Pipe interface.")
	}
}

func TestPipe_OnForwardForForward(t *testing.T) {
	called := false

	msg := streams.NewMessage("test", "test")
	p := NewPipe(t)
	p.OnForward(func(m *streams.Message) {
		called = true
		assert.Equal(t, msg, m)
	})

	p.Forward(msg)

	assert.True(t, called)
}

func TestPipe_OnForwardForForwardToChild(t *testing.T) {
	called := false

	msg := streams.NewMessage("test", "test")
	p := NewPipe(t)
	p.OnForward(func(m *streams.Message) {
		called = true
		assert.Exactly(t, msg, m)
	})

	p.ForwardToChild(msg, 0)

	assert.True(t, called)
}

func TestPipe_HandlesExpectations(t *testing.T) {
	p := NewPipe(t)

	p.ExpectForward("test", "test")
	p.ExpectForwardToChild("test", "test", 1)
	p.ExpectCommit()

	p.Forward(streams.NewMessage("test", "test"))
	p.ForwardToChild(streams.NewMessage("test", "test"), 1)
	p.Commit(streams.NewMessage("test", "test"))
	p.AssertExpectations()
}

func TestPipe_WithoutExpectationOnForward(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when no expectation on Forward")
		}

	}()
	p := NewPipe(mockT)

	p.Forward(streams.NewMessage("test", "test"))
}

func TestPipe_WithWrongExpectationOnForward(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when wrong expectation on Forward")
		}

	}()
	p := NewPipe(mockT)
	p.ExpectForward(1, 1)

	p.Forward(streams.NewMessage("test", "test"))
}

func TestPipe_WithShouldErrorOnForward(t *testing.T) {
	mockT := new(testing.T)
	p := NewPipe(mockT)
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
	p := NewPipe(mockT)
	p.ExpectForwardToChild(1, 1, 3)

	p.ForwardToChild(streams.NewMessage("test", "test"), 1)
}

func TestPipe_WithShouldErrorOnForwardToChild(t *testing.T) {
	mockT := new(testing.T)
	p := NewPipe(mockT)
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
	p := NewPipe(mockT)

	p.Commit(streams.NewMessage("test", "test"))
}

func TestPipe_WithErrorOnCommit(t *testing.T) {
	mockT := new(testing.T)
	p := NewPipe(mockT)
	p.ExpectCommit()
	p.ShouldError()

	err := p.Commit(streams.NewMessage("test", "test"))

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
	p := NewPipe(mockT)
	p.ExpectForward(1, 1)

	p.AssertExpectations()
}

func TestPipe_WithUnfulfilledExpectationOnForwardToChild(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when unforfilled expectation on ForwardToChild")
		}

	}()
	p := NewPipe(mockT)
	p.ExpectForwardToChild(1, 1, 1)

	p.AssertExpectations()
}

func TestPipe_WithUnfulfilledExpectationOnCommit(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when unforfilled expectation on Commit")
		}

	}()
	p := NewPipe(mockT)
	p.ExpectCommit()

	p.AssertExpectations()
}
