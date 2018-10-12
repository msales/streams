package streams

import (
	"sync"
	"time"

	"github.com/msales/pkg/stats"
	"github.com/pkg/errors"
	"github.com/tevino/abool"
)

type ErrorFunc func(error)

type Task interface {
	Start() error
	OnError(fn ErrorFunc)
	Close() error
}

type streamTask struct {
	topology *Topology

	running *abool.AtomicBool
	errorFn ErrorFunc

	sourceWg sync.WaitGroup
	pumps map[Node]Pump
}

func NewTask(topology *Topology) Task {
	return &streamTask{
		topology: topology,
		running:  abool.New(),
	}
}

func (t *streamTask) Start() error {
	// If we are already running, exit
	if !t.running.SetToIf(false, true) {
		return errors.New("streams: task already started")
	}

	t.setupTopology()

	return nil
}

func (t *streamTask) setupTopology() {
	// TODO: create a slice in reverse order of all nodes

	// TODO: run through the slice, resolving node runners

	for _, node := range t.topology.Processors() {
		pipe := NewProcessorPipe(node)
		node.Processor().WithPipe(pipe)
	}

	for source, node := range t.topology.Sources() {
		pipe := NewProcessorPipe(node)
		node.Processor().WithPipe(pipe)

		t.runSource(source, node)
	}
}

func (t *streamTask) runSource(source Source, node Node) {
	go func() {
		t.sourceWg.Add(1)
		defer t.sourceWg.Done()

		for t.running.IsSet() {
			start := time.Now()

			msg, err := source.Consume()
			if err != nil {
				t.handleError(err)
			}

			if msg.Empty() {
				continue
			}

			stats.Timing(msg.Ctx, "node.latency", time.Since(start), 1.0, "name", node.Name())
			stats.Inc(msg.Ctx, "node.throughput", 1, 1.0, "name", node.Name())

			// Get runner
			// err = runner.Process(msg)
			err = node.Processor().Process(msg)
			if err != nil {
				t.handleError(err)
			}
		}
	}()
}

func (t *streamTask) Close() error {
	t.running.UnSet()
	t.sourceWg.Wait()

	return t.closeTopology()
}

func (t *streamTask) closeTopology() error {
	// TODO: Remove recursion and use instead a slice that can be deduped
	for _, node := range t.topology.Sources() {
		if err := t.closeNode(node); err != nil {
			return err
		}
	}

	for source := range t.topology.Sources() {
		if err := source.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (t *streamTask) closeNode(n Node) error {
	// runner.Close()

	for _, child := range n.Children() {
		if err := t.closeNode(child); err != nil {
			return err
		}
	}

	return nil
}

func (t *streamTask) handleError(err error) {
	t.running.UnSet()

	t.errorFn(err)
}

func (t *streamTask) OnError(fn ErrorFunc) {
	t.errorFn = fn
}
