package streams

import (
	"github.com/pkg/errors"
)

// Pipe allows messages to flow through the processors.
type Pipe interface {
	// Forward queues the data to all processor children in the topology.
	Forward(*Message) error
	// Forward queues the data to the the given processor(s) child in the topology.
	ForwardToChild(*Message, int) error
	// Commit commits the current state in the sources.
	Commit(*Message) error
}

var _ = (Pipe)(&ProcessorPipe{})

// ProcessorPipe represents the pipe for processors.
type ProcessorPipe struct {
	children []Pump
}

// NewProcessorPipe create a new ProcessorPipe instance.
func NewProcessorPipe(children []Pump) *ProcessorPipe {
	return &ProcessorPipe{
		children: children,
	}
}

// Forward queues the data to all processor children in the topology.
func (p *ProcessorPipe) Forward(msg *Message) error {
	for _, child := range p.children {
		if err := child.Process(msg); err != nil {
			return err
		}
	}

	return nil
}

// Forward queues the data to the the given processor(s) child in the topology.
func (p *ProcessorPipe) ForwardToChild(msg *Message, index int) error {
	if index > len(p.children)-1 {
		return errors.New("streams: child index out of bounds")
	}

	child := p.children[index]
	return child.Process(msg)
}

// Commit commits the current state in the sources.
func (p *ProcessorPipe) Commit(msg *Message) error {
	for s, v := range msg.Metadata() {
		if err := s.Commit(v); err != nil {
			return err
		}
	}

	return nil
}
