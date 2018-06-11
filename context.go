package streams

import (
	"context"

	"github.com/pkg/errors"
)

type Context interface {
	context.Context

	Forward(key, value interface{}) error
	ForwardToChild(key, value interface{}, child int) error
	Commit() error
}

type ProcessorContext struct {
	context.Context
	task   Task

	currentNode Node
}

func NewProcessorContext(t Task, ctx context.Context) *ProcessorContext {
	return &ProcessorContext{
		Context: ctx,
		task:   t,
	}
}

func (c *ProcessorContext) Forward(key, value interface{}) error {
	previousNode := c.currentNode
	defer func() { c.currentNode = previousNode }()

	for _, child := range c.currentNode.Children() {
		c.currentNode = child
		if err := child.Process(key, value); err != nil {
			return err
		}
	}

	return nil
}

func (c *ProcessorContext) ForwardToChild(key, value interface{}, index int) error {
	previousNode := c.currentNode
	defer func() { c.currentNode = previousNode }()

	if index > len(c.currentNode.Children())-1 {
		return errors.New("streams: child index out of bounds")
	}

	child := c.currentNode.Children()[index]
	c.currentNode = child
	if err := child.Process(key, value); err != nil {
		return err
	}

	return nil
}

func (c *ProcessorContext) Commit() error {
	return c.task.Commit()
}
