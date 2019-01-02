package streams

import (
	"errors"
	"time"
)

// ErrorFunc represents a streams error handling function.
type ErrorFunc func(error)

// TaskOptFunc represents a function that sets up the Task.
type TaskOptFunc func(t *streamTask)

// WithCommitInterval defines an interval of automatic commits.
func WithCommitInterval(d time.Duration) TaskOptFunc {
	return func(t *streamTask) {
		t.supervisorOpts.Interval = d
	}
}

// WithMetadataStrategy defines an strategy of metadata mergers.
func WithMetadataStrategy(strategy MetadataStrategy) TaskOptFunc {
	return func(t *streamTask) {
		t.supervisorOpts.Strategy = strategy
	}
}

// Task represents a streams task.
type Task interface {
	// Start starts the streams processors.
	Start() error
	// OnError sets the error handler.
	OnError(fn ErrorFunc)
	// Close stops and closes the streams processors.
	Close() error
}

type supervisorOpts struct {
	Strategy MetadataStrategy
	Interval time.Duration
}

type streamTask struct {
	topology *Topology

	running bool
	errorFn ErrorFunc

	store          Metastore
	supervisorOpts supervisorOpts
	supervisor     Supervisor
	srcPumps       SourcePumps
	pumps          map[Node]Pump
}

// NewTask creates a new streams task.
func NewTask(topology *Topology, opts ...TaskOptFunc) Task {
	store := NewMetastore()

	t := &streamTask{
		topology: topology,
		store:    store,
		supervisorOpts: supervisorOpts{
			Strategy: Lossless,
			Interval: 0,
		},
		srcPumps: SourcePumps{},
		pumps:    map[Node]Pump{},
	}

	for _, optFn := range opts {
		optFn(t)
	}

	t.supervisor = NewSupervisor(t.store, t.supervisorOpts.Strategy)
	if t.supervisorOpts.Interval > 0 {
		t.supervisor = NewTimedSupervisor(t.supervisor, t.supervisorOpts.Interval, t.errorFn)
	}

	return t
}

// Start starts the streams processors.
func (t *streamTask) Start() error {
	// If we are already running, exit
	if t.running {
		return errors.New("streams: task already running")
	}
	t.running = true

	t.setupTopology()

	return t.supervisor.Start()
}

func (t *streamTask) setupTopology() {
	nodes := flattenNodeTree(t.topology.Sources())
	reverseNodes(nodes)
	for _, node := range nodes {
		pipe := NewPipe(t.store, t.supervisor, node.Processor(), t.resolvePumps(node.Children()))
		node.Processor().WithPipe(pipe)

		pump := NewPump(node, pipe.(TimedPipe), t.handleError)
		t.pumps[node] = pump
	}

	t.supervisor.WithPumps(t.pumps)

	for source, node := range t.topology.Sources() {
		srcPump := NewSourcePump(node.Name(), source, t.resolvePumps(node.Children()), t.handleError)
		t.srcPumps = append(t.srcPumps, srcPump)
	}
}

func (t *streamTask) resolvePumps(nodes []Node) []Pump {
	var pumps []Pump
	for _, node := range nodes {
		pumps = append(pumps, t.pumps[node])
	}
	return pumps
}

// Close stops and closes the streams processors.
func (t *streamTask) Close() error {
	t.running = false
	t.srcPumps.StopAll()

	return t.closeTopology()
}

func (t *streamTask) closeTopology() error {
	// Stop the pumps
	nodes := flattenNodeTree(t.topology.Sources())
	for _, node := range nodes {
		t.pumps[node].Stop()
	}

	// Commit any outstanding batches and metadata
	if err := t.supervisor.Commit(nil); err != nil {
		return err
	}

	// Close the supervisor
	if err := t.supervisor.Close(); err != nil {
		return err
	}

	// Close the pumps
	for _, node := range nodes {
		if err := t.pumps[node].Close(); err != nil {
			return err
		}
	}

	// Close the sources
	for _, srcPump := range t.srcPumps {
		if err := srcPump.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (t *streamTask) handleError(err error) {
	t.running = false
	t.srcPumps.StopAll()

	t.errorFn(err)
}

// OnError sets the error handler.
func (t *streamTask) OnError(fn ErrorFunc) {
	t.errorFn = fn
}

// Tasks represents a slice of tasks.
// This is a utility type that makes it easier to work with multiple tasks.
type Tasks []Task

// Start starts the streams processors.
func (tasks Tasks) Start() error {
	err := tasks.each(func(t Task) error {
		return t.Start()
	})

	return err
}

// OnError sets the error handler.
func (tasks Tasks) OnError(fn ErrorFunc) {
	_ = tasks.each(func(t Task) error {
		t.OnError(fn)
		return nil
	})
}

// Close stops and closes the streams processors.
// This function operates on the tasks in the reversed order.
func (tasks Tasks) Close() error {
	err := tasks.eachRev(func(t Task) error {
		return t.Close()
	})

	return err

}

// each executes a passed function with every task in the slice.
func (tasks Tasks) each(fn func(t Task) error) error {
	for _, t := range tasks {
		err := fn(t)
		if err != nil {
			return err
		}
	}

	return nil
}

// eachRev executes a passed function with every task in the slice, in the reversed order.
func (tasks Tasks) eachRev(fn func(t Task) error) error {
	for i := len(tasks) - 1; i >= 0; i-- {
		err := fn(tasks[i])
		if err != nil {
			return err
		}
	}

	return nil
}
