package streams

import "sync"

type Task struct {
	topology *Topology

	running bool
	wg      sync.WaitGroup
}

func NewTask(topology *Topology) *Task {
	return &Task{
		topology: topology,
		running:  false,
	}
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
				// TODO: handle
			}

			ctx.node = node
			if err := node.Process(k, v); err != nil {
				// TODO: handle
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

func (t *Task) Start() {
	go t.run()
}

func (t *Task) Commit() error {
	for source := range t.topology.Sources() {
		if err := source.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func (t *Task) Close() {
	t.running = false

	t.wg.Wait()
	t.closeTopology()
}
