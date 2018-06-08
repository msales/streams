package streams

import (
	"sync"

	"github.com/msales/pkg/log"
	"github.com/msales/pkg/stats"
)

type record struct {
	Node  Node
	Key   interface{}
	Value interface{}
}

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
	Commit() error
	OnError(fn ErrorFunc)
	Close() error
}

type streamTask struct {
	topology *Topology

	logger log.Logger
	stats  stats.Stats

	running  bool
	errorFn  ErrorFunc
	records  chan record
	runWg    sync.WaitGroup
	sourceWg sync.WaitGroup
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
	// If we are already running, exit
	if t.running {
		return
	}

	t.records = make(chan record, 1000)
	t.running = true

	ctx := NewProcessorContext(t, t.logger, t.stats)
	t.setupTopology(ctx)

	t.consumeSources()

	t.runWg.Add(1)
	defer t.runWg.Done()

	for r := range t.records {
		ctx.currentNode = r.Node
		if err := r.Node.Process(r.Key, r.Value); err != nil {
			t.handleError(err)
		}
	}
}

func (t *streamTask) setupTopology(ctx Context) {
	for _, n := range t.topology.Processors() {
		n.WithContext(ctx)
	}
}

func (t *streamTask) closeTopology() error {
	for _, node := range t.topology.Processors() {
		if err := node.Close(); err != nil {
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

func (t *streamTask) handleError(err error) {
	t.running = false

	t.errorFn(err)
}

func (t *streamTask) consumeSources() {
	for source, node := range t.topology.Sources() {
		go func(source Source, node Node) {
			t.sourceWg.Add(1)
			defer t.sourceWg.Done()

			for t.running {
				k, v, err := source.Consume()
				if err != nil {
					t.handleError(err)
				}

				if k == nil && v == nil {
					continue
				}

				t.records <- record{
					Node:  node,
					Key:   k,
					Value: v,
				}
			}
		}(source, node)
	}
}

func (t *streamTask) Start() {
	go t.run()
}

func (t *streamTask) Commit() error {
	for source := range t.topology.Sources() {
		if err := source.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func (t *streamTask) OnError(fn ErrorFunc) {
	t.errorFn = fn
}

func (t *streamTask) Close() error {
	t.running = false
	t.sourceWg.Wait()

	close(t.records)
	t.runWg.Wait()

	return t.closeTopology()
}
