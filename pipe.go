package streams

import (
	"github.com/pkg/errors"
)

// Pipe allows messages to flow through the processors.
type Pipe interface {
	// Forward passes the data to all processor children in the topology.
	Forward(*Message) error
	// Forward passes the data to the the given processor(s) child in the topology.
	ForwardToChild(*Message, int) error
	// Commit commits the current state in the sources.
	Commit(*Message) error
}

// ProcessorPipe represents the pipe for processors.
type ProcessorPipe struct {
	node Node
}

// NewProcessorPipe create a new ProcessorPipe instance.
func NewProcessorPipe(node Node) *ProcessorPipe {
	return &ProcessorPipe{
		node: node,
	}
}

// Forward passes the data to all processor children in the topology.
func (p *ProcessorPipe) Forward(msg *Message) error {
	for _, child := range p.node.Children() {
		child.Input() <- msg
	}

	return nil
}

// Forward passes the data to the the given processor(s) child in the topology.
func (p *ProcessorPipe) ForwardToChild(msg *Message, index int) error {
	if index > len(p.node.Children())-1 {
		return errors.New("streams: child index out of bounds")
	}

	child := p.node.Children()[index]
	child.Input() <- msg

	return nil
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
