package streams

import "github.com/pkg/errors"

type Context interface {
	Forward(key, value interface{}) error
	ForwardToChild(key, value interface{}, child int) error
	Commit() error
}

type ProcessorContext struct {
	task *Task
	node Node
}

func NewProcessorContext(t *Task) *ProcessorContext {
	return &ProcessorContext{
		task: t,
	}
}

func (c *ProcessorContext) Forward(key, value interface{}) error {
	previousNode := c.node
	defer func() { c.node = previousNode }()

	for _, child := range c.node.Children() {
		c.node = child
		if err := child.Process(key, value); err != nil {
			return err
		}
	}

	return nil
}

func (c *ProcessorContext) ForwardToChild(key, value interface{}, index int) error {
	previousNode := c.node
	defer func() { c.node = previousNode }()

	if index > len(c.node.Children())-1 {
		return errors.New("streams: child index out of bounds")
	}

	child := c.node.Children()[index]
	c.node = child
	if err := child.Process(key, value); err != nil {
		return err
	}

	return nil
}

func (c *ProcessorContext) Commit() error {
	return c.task.Commit()
}
