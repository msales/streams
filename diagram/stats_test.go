package diagram_test

import (
	"github.com/msales/streams/v2"
	"github.com/msales/streams/v2/diagram"
	"github.com/stretchr/testify/assert"
	"testing"
)

type processorMock struct {
}

func (p *processorMock) WithPipe(pipe streams.Pipe) {
	return
}

func (p *processorMock) Process(msg *streams.Message) error {
	return nil
}

func (p *processorMock) Close() error {
	return nil
}

type sourceMock struct {
	name string
}

func (s *sourceMock) Consume() (*streams.Message, error) {
	return streams.NewMessage(nil, nil), nil
}

func (s *sourceMock) Commit(c interface{}) error {
	return nil
}

func (s *sourceMock) Close() error {
	return nil
}

type filterMock struct {
}

func (f *filterMock) Assert(msg *streams.Message) (bool, error) {
	return true, nil
}

type flatMapperMock struct {
}

func (f *flatMapperMock) FlatMap(msg *streams.Message) ([]*streams.Message, error) {
	out := make([]*streams.Message, 0, 1)

	out = append(out, streams.NewMessage(nil, nil))

	return out, nil
}

func TestStat_getStats(t *testing.T) {
	builder := streams.NewStreamBuilder()
	bucketsStream := builder.Source("bucket-poll", &sourceMock{"test1"}).
		Map("bucket-to-event", &mapperMock{}).
		Process("bucket-processor", &processorMock{})

	builder.Source("event-source", &sourceMock{"test2"}).
		Filter("filter-cubes", &filterMock{}).
		Map("event-deserialize", &mapperMock{}).
		Process("group-processor", &processorMock{}).
		Process("filter-performing", &processorMock{}).
		Process("budget-analyze", &processorMock{}).
		FlatMap("flat-map", &flatMapperMock{}).
		Merge("merge-release", bucketsStream).
		Process("decision-sink", &processorMock{})

	topology, errs := builder.Build()
	assert.Nil(t, errs)

	s := diagram.NewStat(topology)
	expected := `graph LR
A[bucket-poll] --> B[bucket-to-event]
B[bucket-to-event] --> C[bucket-processor]
C[bucket-processor] --> D[merge-release]
D[merge-release] --> E[decision-sink]
F[event-source] --> G[filter-cubes]
G[filter-cubes] --> H[event-deserialize]
H[event-deserialize] --> I[group-processor]
I[group-processor] --> J[filter-performing]
J[filter-performing] --> K[budget-analyze]
K[budget-analyze] --> L[flat-map]
L[flat-map] --> D[merge-release]`

	stats, err := s.GetStats()
	assert.NoError(t, err)
	assert.Equal(t, expected, stats)
}

type mapperMock struct {
}

func (f *mapperMock) Map(msg *streams.Message) (*streams.Message, error) {
	return msg, nil
}
