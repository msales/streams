package mocks

import (
	"errors"
	"testing"

	"github.com/msales/streams/v2"
)

type record struct {
	key   interface{}
	value interface{}
	index int
}

// ChildMessage represents a message forwarded to a child index.
type ChildMessage struct {
	Index int
	Msg   streams.Message
}

var _ = (streams.Pipe)(&Pipe{})

// Pipe is a mock Pipe.
type Pipe struct {
	t *testing.T

	msgs []ChildMessage

	shouldError bool

	expectMark    []record
	expectForward []record
	expectCommit  bool
}

// NewPipe create a new mock Pipe instance.
func NewPipe(t *testing.T) *Pipe {
	return &Pipe{
		t:             t,
		msgs:          []ChildMessage{},
		expectMark:    []record{},
		expectForward: []record{},
	}
}

// Messages gets the queued Messages for each Node.
func (p *Pipe) Messages() []ChildMessage {
	return p.msgs
}

// Mark indicates that the message has been delt with
func (p *Pipe) Mark(msg streams.Message) error {
	if len(p.expectMark) == 0 {
		p.t.Error("streams: mock: Unexpected call to Mark")
		return nil
	}
	record := p.expectMark[0]
	p.expectMark = p.expectMark[1:]

	if (record.key != Anything && msg.Key != record.key) ||
		(record.value != Anything && msg.Value != record.value) {
		p.t.Errorf("streams: mock: Arguments to Mark did not match expectation: wanted %v:%v, got %v:%v", record.key, record.value, msg.Key, msg.Value)
	}

	if p.shouldError {
		p.shouldError = false
		return errors.New("test")
	}

	return nil
}

// Forward queues the data to all processor children in the topology.
func (p *Pipe) Forward(msg streams.Message) error {
	if len(p.expectForward) == 0 {
		p.t.Error("streams: mock: Unexpected call to Forward")
		return nil
	}
	record := p.expectForward[0]
	p.expectForward = p.expectForward[1:]

	if (record.key != Anything && msg.Key != record.key) ||
		(record.value != Anything && msg.Value != record.value) {
		p.t.Errorf("streams: mock: Arguments to Forward did not match expectation: wanted %v:%v, got %v:%v", record.key, record.value, msg.Key, msg.Value)
	}

	if p.shouldError {
		p.shouldError = false
		return errors.New("test")
	}

	p.msgs = append(p.msgs, ChildMessage{Index: -1, Msg: msg})

	return nil
}

// ForwardToChild queues the data to the the given processor(s) child in the topology.
func (p *Pipe) ForwardToChild(msg streams.Message, index int) error {
	if len(p.expectForward) == 0 {
		p.t.Error("streams: mock: Unexpected call to ForwardToChild")
		return nil
	}
	record := p.expectForward[0]
	p.expectForward = p.expectForward[1:]

	if (record.key != Anything && msg.Key != record.key) ||
		(record.value != Anything && msg.Value != record.value) ||
		index != record.index {
		p.t.Errorf("streams: mock: Arguments to Forward did not match expectation: wanted %v:%v:%d, got %v:%v:%d", record.key, record.value, record.index, msg.Key, msg.Value, index)
	}

	if p.shouldError {
		p.shouldError = false
		return errors.New("test")
	}

	p.msgs = append(p.msgs, ChildMessage{Index: index, Msg: msg})

	return nil
}

// Commit commits the current state in the sources.
func (p *Pipe) Commit(msg streams.Message) error {
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

// ShouldError indicates that an error should be returned on the
// next operation.
func (p *Pipe) ShouldError() {
	p.shouldError = true
}

// ExpectMark registers an expectation of a Mark on the Pipe.
func (p *Pipe) ExpectMark(k, v interface{}) {
	p.expectMark = append(p.expectMark, record{k, v, -1})
}

// ExpectForward registers an expectation of a Forward on the Pipe.
func (p *Pipe) ExpectForward(k, v interface{}) {
	p.expectForward = append(p.expectForward, record{k, v, -1})
}

// ExpectForwardToChild registers an expectation of a ForwardToChild on the Pipe.
func (p *Pipe) ExpectForwardToChild(k, v interface{}, index int) {
	p.expectForward = append(p.expectForward, record{k, v, index})
}

// ExpectCommit registers an expectation of a Commit on the Pipe.
func (p *Pipe) ExpectCommit() {
	p.expectCommit = true
}

// AssertExpectations asserts that the expectations were met.
func (p *Pipe) AssertExpectations() {
	if len(p.expectMark) > 0 {
		p.t.Error("streams: mock: Expected a call to Mark but got none")
	}

	if len(p.expectForward) > 0 {
		p.t.Error("streams: mock: Expected a call to Forward or ForwardToChild but got none")
	}

	if p.expectCommit {
		p.t.Error("streams: mock: Expected a call to Commit but got none")
	}
}
