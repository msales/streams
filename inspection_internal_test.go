package streams

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSourcesConnected(t *testing.T) {
	node5 := &testNode{
		children: []Node{},
	}
	node1 := &testNode{
		children: []Node{node5},
	}
	node2 := &testNode{
		children: []Node{node5},
	}

	err := sourcesConnected(map[Source]Node{
		testSource(1): node1,
		testSource(2): node2,
	}, []Node{})

	assert.NoError(t, err)
}

func TestSourcesConnected_Error(t *testing.T) {
	node1 := &testNode{
		children: []Node{},
	}
	node2 := &testNode{
		children: []Node{},
	}

	err := sourcesConnected(map[Source]Node{
		testSource(1): node1,
		testSource(2): node2,
	}, []Node{})

	assert.Error(t, err)
}

func TestCommittersConnected(t *testing.T) {
	node1 := &testNode{
		children:  []Node{},
		processor: &testCommitter{},
	}
	node2 := &testNode{
		children:  []Node{},
		processor: &testCommitter{},
	}

	err := committersConnected(map[Source]Node{}, []Node{node1, node2})

	assert.NoError(t, err)
}

func TestCommittersConnected_Error(t *testing.T) {
	node3 := &testNode{
		children:  []Node{},
		processor: &testCommitter{},
	}
	node2 := &testNode{
		children: []Node{node3},
	}
	node1 := &testNode{
		children:  []Node{node2},
		processor: &testCommitter{},
	}

	err := committersConnected(map[Source]Node{}, []Node{node1, node2, node3})

	assert.Error(t, err)
}

func TestCommitterIsLeafNode(t *testing.T) {
	node2 := &testNode{
		children:  []Node{},
		processor: &testCommitter{},
	}
	node1 := &testNode{
		children:  []Node{node2},
		processor: &testProcessor{},
	}

	err := committerIsLeafNode(map[Source]Node{}, []Node{node1, node2})

	assert.NoError(t, err)
}

func TestCommitterIsLeafNode_Error(t *testing.T) {
	node2 := &testNode{
		children:  []Node{},
		processor: &testProcessor{},
	}
	node1 := &testNode{
		children:  []Node{node2},
		processor: &testCommitter{},
	}

	err := committerIsLeafNode(map[Source]Node{}, []Node{node1, node2})

	assert.Error(t, err)
}
