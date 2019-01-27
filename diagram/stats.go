package diagram

import (
	"bytes"
	"github.com/pkg/errors"
	"strings"

	"github.com/msales/streams/v2"
)

const (
	nodeAppender       = " --> "
	newLine            = "\n"
	defaultDiagramType = "graph LR"
)

type Stat struct {
	topology *streams.Topology

	variablesMap map[string]string
	usedFlows    map[string]string
}

func NewStat(topology *streams.Topology) Stater {
	return &Stat{
		topology:     topology,
		variablesMap: make(map[string]string),
		usedFlows:    make(map[string]string),
	}
}

func (s Stat) GetStats() (string, error) {
	if len(s.topology.Sources()) == 0 {
		return "", errors.New("Empty topology sources given")
	}
	flows := make([]*flow, 0, len(s.topology.Sources()))

	for _, node := range s.topology.Sources() {
		flows = append(flows, s.prepareFlow(node)...)
	}

	diagram := newDiagram(defaultDiagramType, flows)

	return diagram.prepare(), nil
}

func (s *Stat) prepareFlow(n streams.Node) []*flow {
	var flows []*flow
	for _, childNode := range n.Children() {
		if _, exist := s.usedFlows[s.getVariableName(n)]; exist {
			continue
		}

		from := &node{
			name:        s.getVariableName(n),
			description: getDescription(n),
		}

		to := &node{
			name:        s.getVariableName(childNode),
			description: getDescription(childNode),
		}

		flows = append(flows, &flow{
			from: from,
			to:   to,
		})
		s.usedFlows[from.name] = to.name

		flows = append(flows, s.prepareFlow(childNode)...)
	}

	return flows
}

func (s *Stat) getVariableName(n streams.Node) string {
	if _, exist := s.variablesMap[n.Name()]; !exist {
		s.variablesMap[n.Name()] = s.getNextLetter()
	}

	return s.variablesMap[n.Name()]

}

func (s *Stat) getNextLetter() string {
	total := len(s.variablesMap)

	return string(rune('A' + total))
}

func getDescription(n streams.Node) *shape {
	return &shape{shapeType: getShapeType(n), text: n.Name()}
}

func getShapeType(n streams.Node) string {
	return "[]"
}

type diagram struct {
	diagramType string

	flows []*flow
}

func newDiagram(diagramType string, flows []*flow) *diagram {
	return &diagram{diagramType: diagramType, flows: flows}
}

type flow struct {
	from *node
	to   *node
}

func (f *flow) prepare() string {
	var o bytes.Buffer

	o.WriteString(f.from.prepare())
	o.WriteString(nodeAppender)
	o.WriteString(f.to.prepare())

	return o.String()
}

type node struct {
	name        string
	description *shape
}

func (n *node) prepare() string {
	var o bytes.Buffer

	o.WriteString(n.name)
	if n.description != nil {
		o.WriteString(n.description.prepare())
	}

	return o.String()
}

type shape struct {
	text      string
	shapeType string
}

func (s shape) prepare() string {
	var o bytes.Buffer

	shapeBoundings := s.getBoundings()

	leftBound := shapeBoundings[0]
	rightBound := shapeBoundings[1]
	o.WriteString(leftBound)

	o.WriteString(s.text)

	o.WriteString(rightBound)

	return o.String()
}

func (s shape) getBoundings() []string {
	return strings.SplitN(s.shapeType, "", 2)
}

func (d *diagram) prepare() string {
	var o bytes.Buffer

	o.WriteString(d.diagramType)

	for _, flow := range d.flows {
		o.WriteString(newLine)
		o.WriteString(flow.prepare())
	}

	return o.String()
}
