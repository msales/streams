package streams

import (
	"errors"
	"time"
)

// ErrorFunc represents a streams error handling function.
type ErrorFunc func(error)

// Task represents a streams task.
type Task interface {
	// Start starts the streams processors.
	Start() error
	// OnError sets the error handler.
	OnError(fn ErrorFunc)
	// Close stops and closes the streams processors.
	Close() error
}

type streamTask struct {
	topology *Topology

	running bool
	errorFn ErrorFunc

	store      Metastore
	supervisor Supervisor
	srcPumps   SourcePumps
	pumps      map[Node]Pump
}

// NewTask creates a new streams task.
func NewTask(topology *Topology, opts ...TaskOptFunc) Task {
	store := NewMetastore()

	t := &streamTask{
		topology:   topology,
		store:      store,
		supervisor: NewSupervisor(store),
		srcPumps:   SourcePumps{},
		pumps:      map[Node]Pump{},
	}

	for _, optFn := range opts {
		optFn(t)
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
	nodes := flattenNodeTree(t.topology.Sources())
	for _, node := range nodes {
		if err := t.pumps[node].Close(); err != nil {
			return err
		}
	}

	if err := t.supervisor.Commit(nil); err != nil {
		return err
	}

	if err := t.supervisor.Close(); err != nil {
		return err
	}

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

// TaskOptFunc represents a function that sets up the Task.
type TaskOptFunc func(t *streamTask)

// CommitInterval defines an interval of automatic commits.
func CommitInterval(d time.Duration) TaskOptFunc {
	return func(t *streamTask) {
		t.supervisor = NewTimedSupervisor(t.supervisor, d, &t.errorFn)
	}
}
