package streams

import (
	"context"

	"github.com/pkg/errors"
)

type Context interface {
	Forward(context.Context, interface{}, interface{}) error
	ForwardToChild(context.Context, interface{}, interface{}, int) error
	Commit() error
}

type ProcessorContext struct {
	task   Task

	currentNode Node
}

func NewProcessorContext(t Task) *ProcessorContext {
	return &ProcessorContext{
		task:   t,
	}
}

func (c *ProcessorContext) Forward(ctx context.Context, k, v interface{}) error {
	previousNode := c.currentNode
	defer func() { c.currentNode = previousNode }()

	for _, child := range c.currentNode.Children() {
		c.currentNode = child
		if err := child.Process(ctx, k, v); err != nil {
			return err
		}
	}

	return nil
}

func (c *ProcessorContext) ForwardToChild(ctx context.Context, k, v interface{}, index int) error {
	previousNode := c.currentNode
	defer func() { c.currentNode = previousNode }()

	if index > len(c.currentNode.Children())-1 {
		return errors.New("streams: child index out of bounds")
	}

	child := c.currentNode.Children()[index]
	c.currentNode = child
	if err := child.Process(ctx, k, v); err != nil {
		return err
	}

	return nil
}

func (c *ProcessorContext) Commit() error {
	return c.task.Commit()
}
