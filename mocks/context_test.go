package mocks

import (
	"testing"

	"github.com/msales/streams"
)

func TestContext_ImplementsContextInterface(t *testing.T) {
	var c interface{} = &Context{}

	if _, ok := c.(streams.Context); !ok {
		t.Error("The mock context should implement the streams.Context interface.")
	}
}

func TestContext_HandlesExpectations(t *testing.T) {
	c := NewContext(t)

	c.ExpectForward("test", "test")
	c.ExpectForwardToChild("test", "test", 1)
	c.ExpectCommit()

	c.Forward(streams.NewMessage("test", "test"))
	c.ForwardToChild(streams.NewMessage("test", "test"), 1)
	c.Commit()
	c.AssertExpectations()
}

func TestContext_WithoutExpectationOnForward(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when no expectation on Forward")
		}

	}()
	c := NewContext(mockT)

	c.Forward(streams.NewMessage("test", "test"))
}

func TestContext_WithWrongExpectationOnForward(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when wrong expectation on Forward")
		}

	}()
	c := NewContext(mockT)
	c.ExpectForward(1, 1)

	c.Forward(streams.NewMessage("test", "test"))
}

func TestContext_WithShouldErrorOnForward(t *testing.T) {
	mockT := new(testing.T)
	c := NewContext(mockT)
	c.ExpectForward("test", "test")
	c.ShouldError()

	err := c.Forward(streams.NewMessage("test", "test"))

	if err == nil {
		t.Error("Expected error but got none")
	}
}

func TestContext_WithoutExpectationOnForwardToChild(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when no expectation on Forward")
		}

	}()
	c := NewContext(mockT)

	c.ForwardToChild(streams.NewMessage("test", "test"), 1)
}

func TestContextWithWrongExpectationOnForwardToChild(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when wrong expectation on ForwardToChild")
		}

	}()
	c := NewContext(mockT)
	c.ExpectForwardToChild(1, 1, 3)

	c.ForwardToChild(streams.NewMessage("test", "test"), 1)
}

func TestContext_WithShouldErrorOnForwardToChild(t *testing.T) {
	mockT := new(testing.T)
	c := NewContext(mockT)
	c.ExpectForwardToChild("test", "test", 1)
	c.ShouldError()

	err := c.ForwardToChild(streams.NewMessage("test", "test"), 1)

	if err == nil {
		t.Error("Expected error but got none")
	}
}

func TestContext_WithoutExpectationOnCommit(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when no expectation on Commit")
		}

	}()
	c := NewContext(mockT)

	c.Commit()
}

func TestContext_WithErrorOnCommit(t *testing.T) {
	mockT := new(testing.T)
	c := NewContext(mockT)
	c.ExpectCommit()
	c.ShouldError()

	err := c.Commit()

	if err == nil {
		t.Error("Expected error but got none")
	}
}

func TestContext_WithUnfulfilledExpectationOnForward(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when unforfilled expectation on Forward")
		}

	}()
	c := NewContext(mockT)
	c.ExpectForward(1, 1)

	c.AssertExpectations()
}

func TestContext_WithUnfulfilledExpectationOnForwardToChild(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when unforfilled expectation on ForwardToChild")
		}

	}()
	c := NewContext(mockT)
	c.ExpectForwardToChild(1, 1, 1)

	c.AssertExpectations()
}

func TestContext_WithUnfulfilledExpectationOnCommit(t *testing.T) {
	mockT := new(testing.T)
	defer func() {
		if !mockT.Failed() {
			t.Error("Expected error when unforfilled expectation on Commit")
		}

	}()
	c := NewContext(mockT)
	c.ExpectCommit()

	c.AssertExpectations()
}
