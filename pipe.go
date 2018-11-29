package streams

import (
	"time"

	"github.com/pkg/errors"
)

// TimedPipe represents a pipe that can accumulate execution time.
type TimedPipe interface {
	// Reset resets the accumulative pipe duration.
	Reset()
	// Duration returns the accumulative pipe duration.
	Duration() time.Duration
}

// Pipe allows messages to flow through the processors.
type Pipe interface {
	// Mark indicates that the message has been delt with
	Mark(*Message) error
	// Forward queues the message with all processor children in the topology.
	Forward(*Message) error
	// Forward queues the message with the the given processor(s) child in the topology.
	ForwardToChild(*Message, int) error
	// Commit commits the current state in the related sources.
	Commit(*Message) error
}

var _ = (TimedPipe)(&processorPipe{})

// processorPipe represents the pipe for processors.
type processorPipe struct {
	store      Metastore
	supervisor Supervisor
	owner      Processor
	children   []Pump

	duration time.Duration
}

// NewPipe create a new processorPipe instance.
func NewPipe(store Metastore, supervisor Supervisor, owner Processor, children []Pump) Pipe {
	return &processorPipe{
		store:      store,
		supervisor: supervisor,
		owner:      owner,
		children:   children,
	}
}

// Reset resets the accumulative pipe duration.
func (p *processorPipe) Reset() {
	p.duration = 0
}

// Duration returns the accumulative pipe duration.
func (p *processorPipe) Duration() time.Duration {
	return p.duration
}

// Mark indicates that the message has been delt with
func (p *processorPipe) Mark(msg *Message) error {
	return p.store.Mark(p.owner, msg.source, msg.metadata)
}

// Forward queues the data to all processor children in the topology.
func (p *processorPipe) Forward(msg *Message) error {
	defer p.time(time.Now())

	for _, child := range p.children {
		if err := child.Accept(msg); err != nil {
			return err
		}
	}

	return nil
}

// Forward queues the data to the the given processor(s) child in the topology.
func (p *processorPipe) ForwardToChild(msg *Message, index int) error {
	defer p.time(time.Now())

	if index > len(p.children)-1 {
		return errors.New("streams: child index out of bounds")
	}

	child := p.children[index]
	return child.Accept(msg)
}

// Commit commits the current state in the sources.
func (p *processorPipe) Commit(msg *Message) error {
	defer p.time(time.Now())

	if err := p.store.Mark(p.owner, msg.source, msg.metadata); err != nil {
		return err
	}

	return p.supervisor.Commit(p.owner)
}

// time adds the duration of the function to the pipe accumulative duration.
func (p *processorPipe) time(t time.Time) {
	p.duration += time.Since(t)
}
