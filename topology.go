package streams

import (
	"time"

	"github.com/msales/pkg/stats"
)

type Node interface {
	Name() string
	Input() chan *Message
	WithPipe(Pipe)
	Pipe() Pipe
	AddChild(n Node)
	Children() []Node
	Process(*Message) error
	Close() error
}

type SourceNode struct {
	name string
	pipe Pipe

	children []Node
}

func NewSourceNode(name string) *SourceNode {
	return &SourceNode{
		name: name,
	}
}

func (n *SourceNode) Name() string {
	return n.name
}

func (n *SourceNode) Input() chan *Message {
	return nil
}

func (n *SourceNode) WithPipe(pipe Pipe) {
	n.pipe = pipe
}

func (n *SourceNode) Pipe() Pipe {
	return n.pipe
}

func (n *SourceNode) AddChild(node Node) {
	n.children = append(n.children, node)
}

func (n *SourceNode) Children() []Node {
	return n.children
}

func (n *SourceNode) Process(msg *Message) error {
	stats.Inc(msg.Ctx, "node.throughput", 1, 1.0, "name", n.name)

	return n.pipe.Forward(msg)
}

func (n *SourceNode) Close() error {
	return nil
}

type ProcessorNode struct {
	name      string
	msgs      chan *Message
	pipe      Pipe
	processor Processor

	children []Node
}

func NewProcessorNode(name string, p Processor) *ProcessorNode {
	return &ProcessorNode{
		name:      name,
		msgs:      make(chan *Message, 1000),
		processor: p,
	}
}

func (n *ProcessorNode) Name() string {
	return n.name
}

func (n *ProcessorNode) Input() chan *Message {
	return n.msgs
}

func (n *ProcessorNode) WithPipe(pipe Pipe) {
	n.pipe = pipe
	n.processor.WithPipe(pipe)
}

func (n *ProcessorNode) Pipe() Pipe {
	return n.pipe
}

func (n *ProcessorNode) AddChild(node Node) {
	n.children = append(n.children, node)
}

func (n *ProcessorNode) Children() []Node {
	return n.children
}

func (n *ProcessorNode) Process(msg *Message) error {
	start := time.Now()

	stats.Inc(msg.Ctx, "node.throughput", 1, 1.0, "name", n.name)

	if err := n.processor.Process(msg); err != nil {
		return err
	}

	stats.Timing(msg.Ctx, "node.latency", time.Since(start), 1.0, "name", n.name)

	return nil
}

func (n *ProcessorNode) Close() error {
	close(n.msgs)

	return n.processor.Close()
}

type Topology struct {
	sources    map[Source]Node
	processors []Node
}

func (t Topology) Sources() map[Source]Node {
	return t.sources
}

func (t Topology) Processors() []Node {
	return t.processors
}

type TopologyBuilder struct {
	sources    map[Source]Node
	processors []Node
}

func NewTopologyBuilder() *TopologyBuilder {
	return &TopologyBuilder{
		sources:    map[Source]Node{},
		processors: []Node{},
	}
}

func (tb *TopologyBuilder) AddSource(name string, source Source) Node {
	n := NewSourceNode(name)

	tb.sources[source] = n
	tb.processors = append(tb.processors, n)

	return n
}

func (tb *TopologyBuilder) AddProcessor(name string, processor Processor, parents []Node) Node {
	n := NewProcessorNode(name, processor)
	for _, parent := range parents {
		parent.AddChild(n)
	}

	tb.processors = append(tb.processors, n)

	return n
}

func (tb *TopologyBuilder) Build() *Topology {
	return &Topology{
		sources:    tb.sources,
		processors: tb.processors,
	}
}
