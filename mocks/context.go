package mocks

import (
	"context"
	"errors"
	"testing"

)

type record struct {
	key   interface{}
	value interface{}
	index int
}

type Context struct {
	t *testing.T

	shouldError bool

	expectForward []record
	expectCommit  bool
}

func NewContext(t *testing.T) *Context {
	return &Context{
		t:             t,
		expectForward: []record{},
	}
}

func (c *Context) Forward(ctx context.Context, k, v interface{}) error {
	if len(c.expectForward) == 0 {
		c.t.Error("streams: mock: Unexpected call to Forward")
		return nil
	}
	record := c.expectForward[0]
	c.expectForward = c.expectForward[1:]

	if k != record.key || v != record.value {
		c.t.Errorf("streams: mock: Arguments to Forward did not match expectation: wanted %v:%v, got %v:%v", record.key, record.value, k, v)
	}

	if c.shouldError {
		c.shouldError = false
		return errors.New("test")
	}

	return nil
}

func (c *Context) ForwardToChild(ctx context.Context, k, v interface{}, index int) error {
	if len(c.expectForward) == 0 {
		c.t.Error("streams: mock: Unexpected call to ForwardToChild")
		return nil
	}
	record := c.expectForward[0]
	c.expectForward = c.expectForward[1:]

	if k != record.key || v != record.value || index != record.index {
		c.t.Errorf("streams: mock: Arguments to Forward did not match expectation: wanted %v:%v:%d, got %v:%v:%d", record.key, record.value, record.index, k, v, index)
	}

	if c.shouldError {
		c.shouldError = false
		return errors.New("test")
	}

	return nil
}

func (c *Context) Commit() error {
	if !c.expectCommit {
		c.t.Error("streams: mock: Unexpected call to Commit")
	}
	c.expectCommit = false

	if c.shouldError {
		c.shouldError = false
		return errors.New("test")
	}

	return nil
}

func (c *Context) ShouldError() {
	c.shouldError = true
}

func (c *Context) ExpectForward(k, v interface{}) {
	c.expectForward = append(c.expectForward, record{k, v, -1})
}

func (c *Context) ExpectForwardToChild(k, v interface{}, index int) {
	c.expectForward = append(c.expectForward, record{k, v, index})
}

func (c *Context) ExpectCommit() {
	c.expectCommit = true
}

func (c *Context) AssertExpectations() {
	if len(c.expectForward) > 0 {
		c.t.Error("streams: mock: Expected a call to Forward or ForwardToChild but got none")
	}

	if c.expectCommit {
		c.t.Error("streams: mock: Expected a call to Commit but got none")
	}
}
