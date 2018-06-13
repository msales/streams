package streams

import (
	"github.com/pkg/errors"
)

type Context interface {
	Forward(*Message) error
	ForwardToChild(*Message, int) error
	Commit() error
}

type ProcessorContext struct {
	task Task

	currentNode Node
}

// NewProcessorContext create a new ProcessorContext instance.
func NewProcessorContext(t Task) *ProcessorContext {
	return &ProcessorContext{
		task: t,
	}
}

// SetNode sets the topology node that is being processed.
//
// The is only needed by the task and should not be used
// directly. Doing so can have some unexpected results.
func (c *ProcessorContext) SetNode(n Node) {
	c.currentNode = n
}

// Forward passes the data to all processor children in the topology.
func (c *ProcessorContext) Forward(msg *Message) error {
	previousNode := c.currentNode
	defer func() { c.currentNode = previousNode }()

	for _, child := range c.currentNode.Children() {
		c.currentNode = child
		if err := child.Process(msg); err != nil {
			return err
		}
	}

	return nil
}

// Forward passes the data to the the given processor(s) child in the topology.
func (c *ProcessorContext) ForwardToChild(msg *Message, index int) error {
	previousNode := c.currentNode
	defer func() { c.currentNode = previousNode }()

	if index > len(c.currentNode.Children())-1 {
		return errors.New("streams: child index out of bounds")
	}

	child := c.currentNode.Children()[index]
	c.currentNode = child
	if err := child.Process(msg); err != nil {
		return err
	}

	return nil
}

// Commit commits the current state in the sources.
func (c *ProcessorContext) Commit() error {
	return c.task.Commit()
}
