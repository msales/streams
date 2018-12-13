package streams

import "errors"

// inspection represents an inspection that should be performed on the topology.
type inspection func(map[Source]Node, []Node) error

// sourcesConnected checks that all sources in a topology are connected.
func sourcesConnected(srcs map[Source]Node, _ []Node) error {
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

// committersConnected checks that no 2 committers are on the same branch.
func committersConnected(_ map[Source]Node, procs []Node) error {
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

// committerIsLeafNode checks that a committer is always a leaf node.
func committerIsLeafNode(_ map[Source]Node, procs []Node) error {
	for _, node := range procs {
		if _, ok := node.Processor().(Committer); !ok {
			continue
		}

		if len(node.Children()) != 0 {
			return errors.New("streams: committers should be leaf nodes in a stream")
		}
	}

	return nil
}
