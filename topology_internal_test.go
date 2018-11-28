package streams

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSourcesConnectedTest(t *testing.T) {
	node5 := &testNode{
		children:  []Node{},
	}
	node1 := &testNode{
		children: []Node{node5},
	}
	node2 := &testNode{
		children: []Node{node5},
	}

	err := sourcesConnectedTest(map[Source]Node{
		testSource(1): node1,
		testSource(2): node2,
	}, []Node{})

	assert.NoError(t, err)
}

func TestSourcesConnectedTest_Error(t *testing.T) {
	node1 := &testNode{
		children: []Node{},
	}
	node2 := &testNode{
		children: []Node{},
	}

	err := sourcesConnectedTest(map[Source]Node{
		testSource(1): node1,
		testSource(2): node2,
	}, []Node{})

	assert.Error(t, err)
}

func TestCommittersConnectedTests(t *testing.T) {
	node1 := &testNode{
		children: []Node{},
		processor: &testCommitter{},
	}
	node2 := &testNode{
		children: []Node{},
		processor: &testCommitter{},
	}

	err := committersConnectedTests(map[Source]Node{}, []Node{node1, node2,})

	assert.NoError(t, err)
}

func TestCommittersConnectedTests_Error(t *testing.T) {
	node3 := &testNode{
		children:  []Node{},
		processor: &testCommitter{},
	}
	node2 := &testNode{
		children: []Node{node3},
	}
	node1 := &testNode{
		children: []Node{node2},
		processor: &testCommitter{},
	}

	err := committersConnectedTests(map[Source]Node{}, []Node{node1, node2, node3})

	assert.Error(t, err)
}

func TestNodesConnected_Connected(t *testing.T) {
	node5 := &testNode{
		children:  []Node{},
	}
	node3 := &testNode{
		children:  []Node{node5},
	}
	node1 := &testNode{
		children: []Node{node3},
	}
	node4 := &testNode{
		children:  []Node{node5},
	}
	node2 := &testNode{
		children: []Node{node4},
	}

	connected := nodesConnected([]Node{node1, node2})

	assert.True(t, connected)
}

func TestNodesConnected_NotConnected(t *testing.T) {
	node3 := &testNode{
		children:  []Node{},
	}
	node1 := &testNode{
		children: []Node{node3},
	}
	node4 := &testNode{
		children:  []Node{},
	}
	node2 := &testNode{
		children: []Node{node4},
	}

	connected := nodesConnected([]Node{node1, node2})

	assert.False(t, connected)
}

func TestNodesConnected_OneNode(t *testing.T) {
	node := &testNode{
		children: []Node{},
	}

	connected := nodesConnected([]Node{node})

	assert.True(t, connected)
}

func TestFlattenNodeTree(t *testing.T) {
	node7 := &testNode{processor: &testProcessor{}}
	node6 := &testNode{processor: &testProcessor{}}
	node5 := &testNode{
		children:  []Node{node6, node7},
		processor: &testProcessor{},
	}
	node3 := &testNode{
		children:  []Node{node5},
		processor: &testProcessor{},
	}
	node1 := &testNode{
		children: []Node{node3},
	}
	node4 := &testNode{
		children:  []Node{node5},
		processor: &testProcessor{},
	}
	node2 := &testNode{
		children: []Node{node4},
	}

	nodes := flattenNodeTree(map[Source]Node{
		testSource(1): node1,
		testSource(2): node2,
	})

	assert.Equal(t, []Node{node3, node4, node5, node6, node7}, nodes)
}

func TestReverse(t *testing.T) {
	node1 := &testNode{}
	node2 := &testNode{}
	node3 := &testNode{}
	node4 := &testNode{}
	nodes := []Node{node1, node2, node3, node4}

	reverseNodes(nodes)

	assert.Equal(t, []Node{node4, node3, node2, node1}, nodes)
}

func TestContains(t *testing.T) {
	node1 := &testNode{}
	node2 := &testNode{}
	node3 := &testNode{}
	node4 := &testNode{}

	tests := []struct {
		node  Node
		nodes []Node
		found bool
	}{
		{
			node:  node1,
			nodes: []Node{node1, node2, node3},
			found: true,
		},
		{
			node:  node4,
			nodes: []Node{node1, node2, node3},
			found: false,
		},
	}

	for _, tt := range tests {
		found := contains(tt.node, tt.nodes)

		assert.Equal(t, tt.found, found)
	}
}

type testNode struct {
	name      string
	children  []Node
	processor Processor
}

func (t *testNode) Name() string {
	return ""
}

func (t *testNode) AddChild(n Node) {
	t.children = append(t.children, n)
}

func (t *testNode) Children() []Node {
	return t.children
}

func (t *testNode) Processor() Processor {
	return t.processor
}

type testSource int

func (s testSource) Consume() (*Message, error) {
	return nil, nil
}

func (s testSource) Commit(v interface{}) error {
	return nil
}

func (s testSource) Close() error {
	return nil
}

type testProcessor struct{}

func (p testProcessor) WithPipe(Pipe) {}

func (p testProcessor) Process(msg *Message) error {
	return nil
}

func (p testProcessor) Close() error {
	return nil
}

type testCommitter struct{}

func (p testCommitter) WithPipe(Pipe) {}

func (p testCommitter) Process(msg *Message) error {
	return nil
}

func (p testCommitter) Commit() error {
	return nil
}

func (p testCommitter) Close() error {
	return nil
}
