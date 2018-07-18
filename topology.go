package streams

import (
	"time"

	"github.com/msales/pkg/stats"
)

type Node interface {
	Name() string
	WithPipe(Pipe)
	AddChild(n Node)
	Children() []Node
	Process(*Message) error
	Close() error
}

type SourceNode struct {
	name      string
	pipe      Pipe
	throttler Throttler

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

func (n *SourceNode) WithPipe(pipe Pipe) {
	n.pipe = pipe
}

func (n *SourceNode) AddChild(node Node) {
	n.children = append(n.children, node)
}

func (n *SourceNode) Children() []Node {
	return n.children
}

func (n *SourceNode) Process(msg *Message) error {
	if should, rate := n.throttle(); !should {
		stats.Inc(msg.Ctx, "node.throughput", 1, rate, "name", n.name)
	}
	return n.pipe.Forward(msg)
}

func (n *SourceNode) Close() error {
	return nil
}

func (n *SourceNode) throttle() (should bool, rate float32) {
	rate = 1

	if n.throttler != nil {
		should = n.throttler.Throttle()
		rate = n.throttler.Rate()
	}

	return
}

type ProcessorNode struct {
	name      string
	processor Processor
	throttler Throttler

	children []Node
}

func NewProcessorNode(name string, p Processor) *ProcessorNode {
	return &ProcessorNode{
		name:      name,
		processor: p,
	}
}

func (n *ProcessorNode) Name() string {
	return n.name
}

func (n *ProcessorNode) WithPipe(pipe Pipe) {
	n.processor.WithPipe(pipe)
}

func (n *ProcessorNode) AddChild(node Node) {
	n.children = append(n.children, node)
}

func (n *ProcessorNode) Children() []Node {
	return n.children
}

func (n *ProcessorNode) Process(msg *Message) error {
	start := time.Now()
	should, rate := n.throttle()

	if !should {
		stats.Inc(msg.Ctx, "node.throughput", 1, rate, "name", n.name)
	}

	if err := n.processor.Process(msg); err != nil {
		return err
	}

	if !should {
		stats.Timing(msg.Ctx, "node.latency", time.Since(start), rate, "name", n.name)
	}

	return nil
}

func (n *ProcessorNode) Close() error {
	return n.processor.Close()
}

func (n *ProcessorNode) throttle() (should bool, rate float32) {
	rate = 1

	if n.throttler != nil {
		should = n.throttler.Throttle()
		rate = n.throttler.Rate()
	}

	return
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
	statsRate  float32
}

func NewTopologyBuilder() *TopologyBuilder {
	return &TopologyBuilder{
		sources:    map[Source]Node{},
		processors: []Node{},
	}
}

func (tb *TopologyBuilder) WithStatsThrottle(rate float32) {
	tb.statsRate = rate
}

func (tb *TopologyBuilder) AddSource(name string, source Source) Node {
	n := &SourceNode{
		name:      name,
		throttler: tb.throttler(),
	}

	tb.sources[source] = n
	tb.processors = append(tb.processors, n)

	return n
}

func (tb *TopologyBuilder) AddProcessor(name string, processor Processor, parents []Node) Node {
	n := &ProcessorNode{
		name:      name,
		processor: processor,
		children:  []Node{},
		throttler: tb.throttler(),
	}

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

func (tb *TopologyBuilder) throttler() Throttler {
	if tb.statsRate > 0 {
		return NewRateThrottler(tb.statsRate)
	}

	return NoopThrottler{}
}
