package streams

import (
	"github.com/pkg/errors"
)

// Pipe allows messages to flow through the processors.
type Pipe interface {
	// Queue gets the queued Messages for each Node.
	Queue() []NodeMessage
	// Forward queues the data to all processor children in the topology.
	Forward(*Message) error
	// Forward queues the data to the the given processor(s) child in the topology.
	ForwardToChild(*Message, int) error
	// Commit commits the current state in the sources.
	Commit(*Message) error
}

// ProcessorPipe represents the pipe for processors.
type ProcessorPipe struct {
	node  Node
	queue []NodeMessage
}

// NewProcessorPipe create a new ProcessorPipe instance.
func NewProcessorPipe(node Node) *ProcessorPipe {
	return &ProcessorPipe{
		node:  node,
		queue: []NodeMessage{},
	}
}

// Queue gets the queued Messages for each Node.
//
// Reading the node message queue will reset the queue.
func (p *ProcessorPipe) Queue() []NodeMessage {
	defer func() {
		p.queue = p.queue[:0]
	}()

	return p.queue
}

// Forward queues the data to all processor children in the topology.
func (p *ProcessorPipe) Forward(msg *Message) error {
	for _, child := range p.node.Children() {
		p.queue = append(p.queue, NodeMessage{child, msg})
	}

	return nil
}

// Forward queues the data to the the given processor(s) child in the topology.
func (p *ProcessorPipe) ForwardToChild(msg *Message, index int) error {
	if index > len(p.node.Children())-1 {
		return errors.New("streams: child index out of bounds")
	}

	child := p.node.Children()[index]
	p.queue = append(p.queue, NodeMessage{child, msg})

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
