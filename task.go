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
		pumps:    map[Node]Pump{},
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
	nodes := flattenNodeTree(t.topology.Sources())
	reverseNodes(nodes)
	for _, node := range nodes {
		pump := NewProcessorPump(node, t.errorFn)
		t.pumps[node] = pump

		pipe := NewProcessorPipe(t.resolvePumps(node.Children()))
		node.Processor().WithPipe(pipe)
	}

	for source, node := range t.topology.Sources() {
		t.runSource(node.Name(), source, t.resolvePumps(node.Children()))
	}
}

func (t *streamTask) resolvePumps(nodes []Node) []Pump {
	var pumps []Pump
	for _, node := range nodes {
		pumps = append(pumps, t.pumps[node])
	}
	return pumps
}

func (t *streamTask) runSource(name string, source Source, pumps []Pump) {
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

			stats.Timing(msg.Ctx, "node.latency", time.Since(start), 1.0, "name", name)
			stats.Inc(msg.Ctx, "node.throughput", 1, 1.0, "name", name)

			for _, pump := range pumps {
				err = pump.Process(msg)
				if err != nil {
					t.handleError(err)
				}
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
	nodes := flattenNodeTree(t.topology.Sources())
	for _, node := range nodes {
		pump := t.pumps[node]
		pump.Close()
	}

	for source := range t.topology.Sources() {
		if err := source.Close(); err != nil {
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
