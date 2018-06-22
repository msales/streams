package mocks

import (
	"errors"
	"testing"

	"github.com/msales/streams"
)

type record struct {
	key   interface{}
	value interface{}
	index int
}

type ForwardFunc func(message *streams.Message)

type Pipe struct {
	t *testing.T

	fn ForwardFunc

	shouldError bool

	expectForward []record
	expectCommit  bool
}

func NewPipe(t *testing.T) *Pipe {
	return &Pipe{
		t:             t,
		expectForward: []record{},
	}
}

func (p *Pipe) Forward(msg *streams.Message) error {
	if p.fn != nil {
		p.fn(msg)

		return nil
	}

	if len(p.expectForward) == 0 {
		p.t.Error("streams: mock: Unexpected call to Forward")
		return nil
	}
	record := p.expectForward[0]
	p.expectForward = p.expectForward[1:]

	if msg.Key != record.key || msg.Value != record.value {
		p.t.Errorf("streams: mock: Arguments to Forward did not match expectation: wanted %v:%v, got %v:%v", record.key, record.value, msg.Key, msg.Value)
	}

	if p.shouldError {
		p.shouldError = false
		return errors.New("test")
	}

	return nil
}

func (p *Pipe) ForwardToChild(msg *streams.Message, index int) error {
	if p.fn != nil {
		p.fn(msg)

		return nil
	}

	if len(p.expectForward) == 0 {
		p.t.Error("streams: mock: Unexpected call to ForwardToChild")
		return nil
	}
	record := p.expectForward[0]
	p.expectForward = p.expectForward[1:]

	if msg.Key != record.key || msg.Value != record.value || index != record.index {
		p.t.Errorf("streams: mock: Arguments to Forward did not match expectation: wanted %v:%v:%d, got %v:%v:%d", record.key, record.value, record.index, msg.Key, msg.Value, index)
	}

	if p.shouldError {
		p.shouldError = false
		return errors.New("test")
	}

	return nil
}

func (p *Pipe) Commit(msg *streams.Message) error {
	if !p.expectCommit {
		p.t.Error("streams: mock: Unexpected call to Commit")
	}
	p.expectCommit = false

	if p.shouldError {
		p.shouldError = false
		return errors.New("test")
	}

	return nil
}

func (p *Pipe) ShouldError() {
	p.shouldError = true
}

func (p *Pipe) OnForward(fn ForwardFunc) {
	p.fn = fn
}

func (p *Pipe) ExpectForward(k, v interface{}) {
	p.expectForward = append(p.expectForward, record{k, v, -1})
}

func (p *Pipe) ExpectForwardToChild(k, v interface{}, index int) {
	p.expectForward = append(p.expectForward, record{k, v, index})
}

func (p *Pipe) ExpectCommit() {
	p.expectCommit = true
}

func (p *Pipe) AssertExpectations() {
	if len(p.expectForward) > 0 {
		p.t.Error("streams: mock: Expected a call to Forward or ForwardToChild but got none")
	}

	if p.expectCommit {
		p.t.Error("streams: mock: Expected a call to Commit but got none")
	}
}
