package streams

import (
	"sync"
	"time"

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
	procWg  sync.WaitGroup
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
	for _, n := range t.topology.Processors() {
		pipe := NewProcessorPipe(n)
		n.WithPipe(pipe)

		t.runProcessor(n)
	}

	for s, n := range t.topology.Sources() {
		pipe := NewProcessorPipe(n)
		n.WithPipe(pipe)

		t.runSource(s, n)
	}
}

func (t *streamTask) runProcessor(n Node) {
	go func() {
		t.procWg.Add(1)
		defer t.procWg.Done()

		for t.running.IsSet() {
			select {
			case msg := <-n.Input():
				if err := n.Process(msg); err != nil {
					t.handleError(err)
				}

			case <-time.After(100 * time.Millisecond):
			}
		}
	}()
}

func (t *streamTask) runSource(s Source, n Node) {
	go func() {
		t.procWg.Add(1)
		defer t.procWg.Done()

		for t.running.IsSet() {
			msg, err := s.Consume()
			if err != nil {
				t.handleError(err)
			}

			if msg.Empty() {
				continue
			}

			if err := n.Process(msg); err != nil {
				t.handleError(err)
			}
		}
	}()
}

func (t *streamTask) Close() error {
	t.running.UnSet()
	t.procWg.Wait()

	return t.closeTopology()
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
	t.running.UnSet()

	t.errorFn(err)
}

func (t *streamTask) OnError(fn ErrorFunc) {
	t.errorFn = fn
}
