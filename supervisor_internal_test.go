package streams

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type ctxKey int

func TestSupervisor_WithContext(t *testing.T) {
	ctx := context.WithValue(context.Background(), ctxKey(1), "test")

	supervisor := &supervisor{}

	supervisor.WithContext(ctx)

	assert.Equal(t, ctx, supervisor.ctx)
}

func TestSupervisor_WithMonitor(t *testing.T) {
	mon := &fakeMonitor{}

	supervisor := &supervisor{}

	supervisor.WithMonitor(mon)

	assert.Equal(t, mon, supervisor.mon)
}

func TestSupervisor_WithPumps(t *testing.T) {
	supervisor := &supervisor{}

	pump := &fakePump{}
	processor := &fakeProcessor{}
	node := newFakeNode(processor)

	input := map[Node]Pump{
		node: pump,
	}

	expected := map[Processor]Pump{
		processor: pump,
	}

	supervisor.WithPumps(input)

	assert.Equal(t, expected, supervisor.pumps)
}

func TestSupervisor_Commit_CommitPending(t *testing.T) {
	supervisor := &supervisor{}
	supervisor.commitMu.Lock()

	err := supervisor.Commit(nil)

	assert.NoError(t, err)
}

type fakeMonitor struct{}

func (*fakeMonitor) Processed(name string, l time.Duration, bp float64) {}

func (*fakeMonitor) Committed(l time.Duration) {}

func (*fakeMonitor) Close() error {
	return nil
}

type fakePump struct{}

func (*fakePump) Lock() {}

func (*fakePump) Unlock() {}

func (*fakePump) Accept(Message) error {
	return nil
}

func (*fakePump) Stop() {
}

func (*fakePump) Close() error {
	return nil
}

type fakeProcessor struct{}

func (*fakeProcessor) WithPipe(Pipe) {}

func (*fakeProcessor) Process(Message) error {
	return nil
}

func (*fakeProcessor) Close() error {
	return nil
}

type fakeNode struct {
	p Processor
}

func newFakeNode(p Processor) *fakeNode {
	return &fakeNode{p: p}
}

func (*fakeNode) Name() string {
	return ""
}

func (*fakeNode) AddChild(n Node) {}

func (*fakeNode) Children() []Node {
	return nil
}

func (n *fakeNode) Processor() Processor {
	return n.p
}
