package diagram

import (
	"testing"

	"github.com/magiconair/properties/assert"
)

func Test_flow_prepareBasic(t *testing.T) {

	flows := []*flow{
		{
			from: &node{
				name: "A",
				description: &shape{
					text:      "First node",
					shapeType: "[]",
				},
			},
			to: &node{
				name: "B",
				description: &shape{
					text:      "Second node",
					shapeType: "()",
				},
			},
		},
		{
			from: &node{
				name: "B",
			},
			to: &node{
				name: "C",
				description: &shape{
					text:      "Third node",
					shapeType: "{}",
				},
			},
		},
	}

	diagram := newDiagram("graph LR", flows)

	diagramString := diagram.prepare()

	responseDiagramString := `graph LR
A[First node] --> B(Second node)
B --> C{Third node}`
	assert.Equal(t, diagramString, responseDiagramString)
}

func Test_flow_prepareWithMerge(t *testing.T) {

	flows := []*flow{
		{
			from: &node{
				name: "A",
				description: &shape{
					text:      "First node",
					shapeType: "[]",
				},
			},
			to: &node{
				name: "B",
				description: &shape{
					text:      "Second node",
					shapeType: "()",
				},
			},
		},
		{
			from: &node{
				name: "B",
			},
			to: &node{
				name: "C",
				description: &shape{
					text:      "Third node",
					shapeType: "{}",
				},
			},
		},
		{
			from: &node{
				name: "D",
				description: &shape{
					text:      "Second source",
					shapeType: "[]",
				},
			},
			to: &node{
				name: "E",
				description: &shape{
					text:      "Second node from second source",
					shapeType: "{}",
				},
			},
		},
		{
			from: &node{
				name: "E",
			},
			to: &node{
				name: "B",
			},
		},
	}

	diagram := newDiagram("graph LR", flows)

	diagramString := diagram.prepare()

	responseDiagramString := `graph LR
A[First node] --> B(Second node)
B --> C{Third node}
D[Second source] --> E{Second node from second source}
E --> B`
	assert.Equal(t, diagramString, responseDiagramString)
}
