package streams

import (
	"github.com/msales/pkg/log"
	"github.com/msales/pkg/stats"
	"github.com/pkg/errors"
)

type Context interface {
	Forward(key, value interface{}) error
	ForwardToChild(key, value interface{}, child int) error
	Commit() error

	Logger() log.Logger
	Stats() stats.Stats
}

type ProcessorContext struct {
	task  *Task
	stats stats.Stats
	logger log.Logger

	currentNode Node
}

func NewProcessorContext(t *Task) *ProcessorContext {
	return &ProcessorContext{
		task:  t,
		logger: t.logger,
		stats: t.stats,
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

func (c *ProcessorContext) Logger() log.Logger {
	return c.logger
}

func (c *ProcessorContext) Stats() stats.Stats {
	return c.stats
}
