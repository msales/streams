package streams

import (
	"sync"

	"github.com/msales/pkg/log"
	"github.com/msales/pkg/stats"
)

type ErrorFunc func(error)

type TaskFunc func(*Task)

func WithLogger(logger log.Logger) TaskFunc {
	return func(t *Task) {
		t.logger = logger
	}
}

func WithStats(stats stats.Stats) TaskFunc {
	return func(t *Task) {
		t.stats = stats
	}
}

type Task struct {
	topology *Topology

	logger log.Logger
	stats  stats.Stats

	running bool
	errorFn ErrorFunc
	wg      sync.WaitGroup
}

func NewTask(topology *Topology, opts ...TaskFunc) *Task {
	t := &Task{
		topology: topology,
		logger:   log.Null,
		stats:    stats.Null,
		running:  false,
	}

	for _, opt := range opts {
		opt(t)
	}

	return t
}

func (t *Task) run() {
	t.running = true
	t.wg.Add(1)
	defer t.wg.Done()

	ctx := NewProcessorContext(t)
	t.setupTopology(ctx)

	for t.running == true {
		for source, node := range t.topology.Sources() {
			k, v, err := source.Consume()
			if err != nil {
				t.handleError(err)
			}

			ctx.currentNode = node
			if err := node.Process(k, v); err != nil {
				t.handleError(err)
			}
		}
	}
}

func (t *Task) setupTopology(ctx Context) {
	for _, n := range t.topology.Processors() {
		n.WithContext(ctx)
	}
}

func (t *Task) closeTopology() {
	for _, node := range t.topology.Processors() {
		node.Close()
	}

	for source := range t.topology.Sources() {
		source.Close()
	}
}

func (t *Task) handleError(err error) {
	t.running = false

	t.errorFn(err)
}

func (t *Task) Start() {
	go t.run()
}

func (t *Task) Commit(sync bool) error {
	for source := range t.topology.Sources() {
		if err := source.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func (t *Task) OnError(fn ErrorFunc) {
	t.errorFn = fn
}

func (t *Task) Close() {
	t.running = false

	t.wg.Wait()
	t.closeTopology()
}
