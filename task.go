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
	procChs  map[Node]chan *Message
	procSigs map[Node]chan bool
}

func NewTask(topology *Topology) Task {
	return &streamTask{
		topology: topology,
		running:  abool.New(),
		procChs:  make(map[Node]chan *Message),
		procSigs: make(map[Node]chan bool),
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
	for _, node := range t.topology.Processors() {
		pipe := NewProcessorPipe(node)
		node.WithPipe(pipe)

		ch := make(chan *Message, 1000)
		t.procChs[node] = ch
		t.procSigs[node] = t.runProcessor(node, ch)
	}

	for source, node := range t.topology.Sources() {
		pipe := NewProcessorPipe(node)
		node.WithPipe(pipe)

		t.runSource(source, node)
	}
}

func (t *streamTask) runProcessor(node Node, ch chan *Message) chan bool {
	done := make(chan bool, 1)

	go func() {
		for msg := range ch {
			start := time.Now()

			nodeMsgs, err := node.Process(msg)
			if err != nil {
				t.handleError(err)
			}

			stats.Timing(msg.Ctx, "node.latency", time.Since(start), 1.0, "name", node.Name())
			stats.Inc(msg.Ctx, "node.throughput", 1, 1.0, "name", node.Name())
			stats.Gauge(msg.Ctx, "node.back-pressure", pressure(ch), 0.1, "name", node.Name())

			for _, nodeMsg := range nodeMsgs {
				ch := t.procChs[nodeMsg.Node]
				ch <- nodeMsg.Msg
			}
		}

		done <- true
	}()

	return done
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

			nodeMsgs, err := node.Process(msg)
			if err != nil {
				t.handleError(err)
			}

			for _, nodeMsg := range nodeMsgs {
				ch := t.procChs[nodeMsg.Node]
				ch <- nodeMsg.Msg
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
	if done, ok := t.procSigs[n]; ok {
		ch := t.procChs[n]
		close(ch)
		<-done
	}

	if err := n.Close(); err != nil {
		return err
	}

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

func pressure(ch chan *Message) float64 {
	l := float64(len(ch))
	c := float64(cap(ch))

	return l / c * 100
}
