package streams

import (
	"time"

	"golang.org/x/xerrors"
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
	Mark(Message) error
	// Forward queues the message with all processor children in the topology.
	Forward(Message) error
	// Forward queues the message with the the given processor(inner) child in the topology.
	ForwardToChild(Message, int) error
	// Commit commits the current state in the related sources.
	Commit(Message) error
}

var _ = (TimedPipe)(&processorPipe{})

// processorPipe represents the pipe for processors.
type processorPipe struct {
	store      Metastore
	supervisor Supervisor
	proc       Processor
	children   []Pump

	duration time.Duration
}

// NewPipe create a new processorPipe instance.
func NewPipe(store Metastore, supervisor Supervisor, proc Processor, children []Pump) Pipe {
	return &processorPipe{
		store:      store,
		supervisor: supervisor,
		proc:       proc,
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
func (p *processorPipe) Mark(msg Message) error {
	start := nanotime()

	err := p.store.Mark(p.proc, msg.source, msg.metadata)

	p.time(start)

	return err
}

// Forward queues the data to all processor children in the topology.
func (p *processorPipe) Forward(msg Message) error {
	start := nanotime()

	for _, child := range p.children {
		if err := child.Accept(msg); err != nil {
			return err
		}
	}

	p.time(start)

	return nil
}

// Forward queues the data to the the given processor(inner) child in the topology.
func (p *processorPipe) ForwardToChild(msg Message, index int) error {
	start := nanotime()

	if index > len(p.children)-1 {
		return xerrors.New("streams: child index out of bounds")
	}

	child := p.children[index]
	err := child.Accept(msg)

	p.time(start)

	return err
}

// Commit commits the current state in the sources.
func (p *processorPipe) Commit(msg Message) error {
	start := nanotime()

	if err := p.store.Mark(p.proc, msg.source, msg.metadata); err != nil {
		return err
	}

	err := p.supervisor.Commit(p.proc)

	p.time(start)

	return err
}

// time adds the duration of the function to the pipe accumulative duration.
func (p *processorPipe) time(t int64) {
	p.duration += time.Duration(nanotime() - t) //time.Since(t)
}
