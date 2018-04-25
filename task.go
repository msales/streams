package streams

import (
	"sync"

	"github.com/msales/pkg/log"
	"github.com/msales/pkg/stats"
)

type ErrorFunc func(error)

type TaskFunc func(*streamTask)

func WithLogger(logger log.Logger) TaskFunc {
	return func(t *streamTask) {
		t.logger = logger
	}
}

func WithStats(stats stats.Stats) TaskFunc {
	return func(t *streamTask) {
		t.stats = stats
	}
}

type Task interface {
	Start()
	Commit(sync bool) error
	OnError(fn ErrorFunc)
	Close()
}

type streamTask struct {
	topology *Topology

	logger log.Logger
	stats  stats.Stats

	running bool
	errorFn ErrorFunc
	wg      sync.WaitGroup
}

func NewTask(topology *Topology, opts ...TaskFunc) Task {
	t := &streamTask{
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

func (t *streamTask) run() {
	t.running = true
	t.wg.Add(1)
	defer t.wg.Done()

	ctx := NewProcessorContext(t, t.logger, t.stats)
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

func (t *streamTask) setupTopology(ctx Context) {
	for _, n := range t.topology.Processors() {
		n.WithContext(ctx)
	}
}

func (t *streamTask) closeTopology() {
	for _, node := range t.topology.Processors() {
		node.Close()
	}

	for source := range t.topology.Sources() {
		source.Close()
	}
}

func (t *streamTask) handleError(err error) {
	t.running = false

	t.errorFn(err)
}

func (t *streamTask) Start() {
	go t.run()
}

func (t *streamTask) Commit(sync bool) error {
	for source := range t.topology.Sources() {
		if err := source.Commit(sync); err != nil {
			return err
		}
	}

	return nil
}

func (t *streamTask) OnError(fn ErrorFunc) {
	t.errorFn = fn
}

func (t *streamTask) Close() {
	t.running = false

	t.wg.Wait()
	t.closeTopology()
}
