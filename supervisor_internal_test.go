package streams

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSupervisor_WithPumps(t *testing.T) {
	supervisor := &supervisor{}

	pump := &fakePump{}
	processor := &fakeProcessor{}
	node := newMockNode(processor)

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

func newMockNode(p Processor) *fakeNode {
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
