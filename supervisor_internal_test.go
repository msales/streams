package streams

import (
	"testing"

	"github.com/magiconair/properties/assert"
)

func TestSupervisor_WithPumps(t *testing.T) {
	supervisor := &supervisor{}

	pump := &mockPump{}
	processor := &mockProcessor{}
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

type mockPump struct {}

func (*mockPump) Accept(*Message) error {
	return nil
}

func (*mockPump) Close() error {
	return nil
}

func (*mockPump) WithLock(func() error) error {
	return nil
}

type mockProcessor struct {}

func (*mockProcessor) WithPipe(Pipe) {}

func (*mockProcessor) Process(*Message) error {
	return nil
}

func (*mockProcessor) Close() error {
	return nil
}

type mockNode struct {
	p Processor
}

func newMockNode(p Processor) *mockNode {
	return &mockNode{p: p}
}

func (*mockNode) Name() string {
	return ""
}

func (*mockNode) AddChild(n Node) {}

func (*mockNode) Children() []Node {
	return nil
}

func (n *mockNode) Processor() Processor {
	return n.p
}