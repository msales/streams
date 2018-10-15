package streams

type Node interface {
	Name() string
	AddChild(n Node)
	Children() []Node
	Processor() Processor
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

func (n *SourceNode) WithPipe(pipe Pipe) {
	n.pipe = pipe
}

func (n *SourceNode) AddChild(node Node) {
	n.children = append(n.children, node)
}

func (n *SourceNode) Children() []Node {
	return n.children
}

func (n *SourceNode) Processor() Processor {
	return nil
}

func (n *SourceNode) Close() error {
	return nil
}

type ProcessorNode struct {
	name      string
	processor Processor

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

func (n *ProcessorNode) AddChild(node Node) {
	n.children = append(n.children, node)
}

func (n *ProcessorNode) Children() []Node {
	return n.children
}

func (n *ProcessorNode) Processor() Processor {
	return n.processor
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

func flattenNodeTree(roots map[Source]Node) []Node {
	var nodes []Node
	var visit []Node

	for _, node := range roots {
		visit = append(visit, node)
	}

	for len(visit) > 0 {
		var n Node
		n, visit = visit[0], visit[1:]

		if _, ok := n.(*SourceNode); !ok {
			nodes = append(nodes, n)
		}

		for _, c := range n.Children() {
			if contains(c, visit) {
				continue
			}

			visit = append(visit, c)
		}
	}

	return nodes
}

func reverseNodes(nodes []Node) {
	for i := len(nodes)/2 - 1; i >= 0; i-- {
		opp := len(nodes) - 1 - i
		nodes[i], nodes[opp] = nodes[opp], nodes[i]
	}
}

func contains(n Node, nodes []Node) bool {
	for _, node := range nodes {
		if node == n {
			return true
		}
	}

	return false
}
