package streams

import (
	"github.com/pkg/errors"
)

// Pipe allows messages to flow through the processors.
type Pipe interface {
	Forward(*Message) error
	ForwardToChild(*Message, int) error
	Commit(*Message) error
}

// ProcessorPipe represents the pipe for processors.
type ProcessorPipe struct {
	currentNode Node
}

// NewProcessorPipe create a new ProcessorPipe instance.
func NewProcessorPipe() *ProcessorPipe {
	return &ProcessorPipe{}
}

// SetNode sets the topology node that is being processed.
//
// The is only needed by the task and should not be used
// directly. Doing so can have some unexpected results.
func (p *ProcessorPipe) SetNode(n Node) {
	p.currentNode = n
}

// Forward passes the data to all processor children in the topology.
func (p *ProcessorPipe) Forward(msg *Message) error {
	previousNode := p.currentNode
	defer func() { p.currentNode = previousNode }()

	for _, child := range p.currentNode.Children() {
		p.currentNode = child
		if err := child.Process(msg); err != nil {
			return err
		}
	}

	return nil
}

// Forward passes the data to the the given processor(s) child in the topology.
func (p *ProcessorPipe) ForwardToChild(msg *Message, index int) error {
	previousNode := p.currentNode
	defer func() { p.currentNode = previousNode }()

	if index > len(p.currentNode.Children())-1 {
		return errors.New("streams: child index out of bounds")
	}

	child := p.currentNode.Children()[index]
	p.currentNode = child
	if err := child.Process(msg); err != nil {
		return err
	}

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
