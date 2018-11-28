package streams

import "errors"

// Node represents a topology node.
type Node interface {
	// Name gets the node name.
	Name() string
	// AddChild adds a child node to the node.
	AddChild(n Node)
	// Children gets the nodes children.
	Children() []Node
	// Processor gets the nodes processor.
	Processor() Processor
}

var _ = (Node)(&SourceNode{})

// SourceNode represents a node between the source
// and the rest of the node tree.
type SourceNode struct {
	name string

	children []Node
}

// NewSourceNode create a new SourceNode.
func NewSourceNode(name string) *SourceNode {
	return &SourceNode{
		name: name,
	}
}

// Name gets the node name.
func (n *SourceNode) Name() string {
	return n.name
}

// AddChild adds a child node to the node.
func (n *SourceNode) AddChild(node Node) {
	n.children = append(n.children, node)
}

// Children gets the nodes children.
func (n *SourceNode) Children() []Node {
	return n.children
}

// Processor gets the nodes processor.
func (n *SourceNode) Processor() Processor {
	return nil
}

var _ = (Node)(&ProcessorNode{})

// ProcessorNode represents the topology node for a processor.
type ProcessorNode struct {
	name      string
	processor Processor

	children []Node
}

// NewProcessorNode creates a new ProcessorNode.
func NewProcessorNode(name string, p Processor) *ProcessorNode {
	return &ProcessorNode{
		name:      name,
		processor: p,
	}
}

// Name gets the node name.
func (n *ProcessorNode) Name() string {
	return n.name
}

// AddChild adds a child node to the node.
func (n *ProcessorNode) AddChild(node Node) {
	n.children = append(n.children, node)
}

// Children gets the nodes children.
func (n *ProcessorNode) Children() []Node {
	return n.children
}

// Processor gets the nodes processor.
func (n *ProcessorNode) Processor() Processor {
	return n.processor
}

// Topology represents the streams topology.
type Topology struct {
	sources    map[Source]Node
	processors []Node
}

// Sources get the topology Sources.
func (t Topology) Sources() map[Source]Node {
	return t.sources
}

// Processors gets the topology Processors.
func (t Topology) Processors() []Node {
	return t.processors
}

// topologyTest represents a test that should be performed on the topology.
type topologyTest func(map[Source]Node, []Node) error

// TopologyBuilder represents a topology builder.
type TopologyBuilder struct {
	tests      []topologyTest
	sources    map[Source]Node
	processors []Node
}

// NewTopologyBuilder creates a new TopologyBuilder.
func NewTopologyBuilder() *TopologyBuilder {
	tests := []topologyTest{
		sourcesConnectedTest,
		committersConnectedTests,
	}

	return &TopologyBuilder{
		tests:      tests,
		sources:    map[Source]Node{},
		processors: []Node{},
	}
}

// AddSource adds a Source to the builder, returning the created Node.
func (tb *TopologyBuilder) AddSource(name string, source Source) Node {
	n := NewSourceNode(name)

	tb.sources[source] = n

	return n
}

// AddProcessor adds a Processor to the builder, returning the created Node.
func (tb *TopologyBuilder) AddProcessor(name string, processor Processor, parents []Node) Node {
	n := NewProcessorNode(name, processor)
	for _, parent := range parents {
		parent.AddChild(n)
	}

	tb.processors = append(tb.processors, n)

	return n
}

// Build creates an immutable Topology.
func (tb *TopologyBuilder) Build() (*Topology, []error) {
	var errs []error
	for _, test := range tb.tests {
		if err := test(tb.sources, tb.processors); err != nil {
			errs = append(errs, err)
		}
	}

	return &Topology{
		sources:    tb.sources,
		processors: tb.processors,
	}, errs
}

func sourcesConnectedTest(srcs map[Source]Node, _ []Node) error {
	nodes := make([]Node, 0, len(srcs))
	for _, node := range srcs {
		nodes = append(nodes, node)
	}

	if len(nodes) <= 1 {
		return nil
	}

	if !nodesConnected(nodes) {
		return errors.New("streams: not all sources are connected")
	}

	return nil
}

func committersConnectedTests(_ map[Source]Node, procs []Node) error {
	var nodes []Node
	for _, node := range procs {
		if _, ok := node.Processor().(Committer); !ok {
			continue
		}

		nodes = append(nodes, node)
	}

	if len(nodes) <= 1 {
		return nil
	}

	if nodesConnected(nodes) {
		return errors.New("streams: committers are inline")
	}

	return nil
}

func nodesConnected(roots []Node) bool {
	if len(roots) <= 1 {
		return true
	}

	var nodes []Node
	var visit []Node
	connections := 0

	for _, node := range roots {
		visit = append(visit, node)
	}

	for len(visit) > 0 {
		var n Node
		n, visit = visit[0], visit[1:]
		nodes = append(nodes, n)

		for _, c := range n.Children() {
			if contains(c, visit) || contains(c, nodes) {
				connections++
				continue
			}

			visit = append(visit, c)
		}
	}

	return connections == len(roots)-1
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

		if n.Processor() != nil {
			nodes = append(nodes, n)
		}

		for _, c := range n.Children() {
			if contains(c, visit) || contains(c, nodes) {
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
