package streams

import (
	"sync"
)

type nodeMessage struct {
	msg  *Message
	node Node
}

type ErrorFunc func(error)

type Task interface {
	Start()
	OnError(fn ErrorFunc)
	Close() error
}

type streamTask struct {
	topology *Topology

	running  bool
	errorFn  ErrorFunc
	stream   chan nodeMessage
	runWg    sync.WaitGroup
	sourceWg sync.WaitGroup
}

func NewTask(topology *Topology) Task {
	return &streamTask{
		topology: topology,
		running:  false,
	}
}

func (t *streamTask) run() {
	// If we are already running, exit
	if t.running {
		return
	}

	t.stream = make(chan nodeMessage, 1000)
	t.running = true

	ctx := NewProcessorPipe()
	t.setupTopology(ctx)

	t.consumeSources()

	t.runWg.Add(1)
	defer t.runWg.Done()

	for r := range t.stream {
		ctx.SetNode(r.node)
		if err := r.node.Process(r.msg); err != nil {
			t.handleError(err)
		}
	}
}

func (t *streamTask) setupTopology(ctx Pipe) {
	for _, n := range t.topology.Processors() {
		n.WithPipe(ctx)
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
				msg, err := source.Consume()
				if err != nil {
					t.handleError(err)
				}

				if msg.Empty() {
					continue
				}

				t.stream <- nodeMessage{
					msg:  msg,
					node: node,
				}
			}
		}(source, node)
	}
}

func (t *streamTask) Start() {
	go t.run()
}

func (t *streamTask) OnError(fn ErrorFunc) {
	t.errorFn = fn
}

func (t *streamTask) Close() error {
	t.running = false
	t.sourceWg.Wait()

	close(t.stream)
	t.runWg.Wait()

	return t.closeTopology()
}
