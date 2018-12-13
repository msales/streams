package streams_test

import (
	"sync"

	"github.com/msales/streams/v2"
)

type fakeSource struct {
	Key   interface{}
	Value interface{}
}

func (s *fakeSource) Consume() (*streams.Message, error) {
	return streams.NewMessage(s.Key, s.Value), nil
}

func (s *fakeSource) Commit(interface{}) error {
	return nil
}

func (s *fakeSource) Close() error {
	return nil
}

type fakeCommitter struct{}

func (*fakeCommitter) WithPipe(streams.Pipe) {}

func (*fakeCommitter) Process(*streams.Message) error {
	return nil
}

func (*fakeCommitter) Commit() error {
	return nil
}

func (*fakeCommitter) Close() error {
	return nil
}

type fakeMetadata struct{}

func (m *fakeMetadata) WithOrigin(streams.MetadataOrigin) {
}

func (m *fakeMetadata) Merge(v streams.Metadata, s streams.MetadataStrategy) streams.Metadata {
	return m
}

type fakeMetastore struct {
	Metadata map[streams.Processor]streams.Metaitems
}

func (ms *fakeMetastore) Pull(p streams.Processor) (streams.Metaitems, error) {
	return ms.Metadata[p], nil
}

func (ms *fakeMetastore) PullAll() (map[streams.Processor]streams.Metaitems, error) {
	return ms.Metadata, nil
}

func (ms *fakeMetastore) Mark(streams.Processor, streams.Source, streams.Metadata) error {
	return nil
}

type fakePump struct {
	sync.Mutex
}

func (*fakePump) Accept(*streams.Message) error {
	return nil
}

func (*fakePump) Stop() {

}

func (*fakePump) Close() error {
	return nil
}

type fakeNode struct {
	Proc streams.Processor
}

func (*fakeNode) Name() string {
	return ""
}

func (*fakeNode) AddChild(n streams.Node) {}

func (*fakeNode) Children() []streams.Node {
	return []streams.Node{}
}

func (n *fakeNode) Processor() streams.Processor {
	return n.Proc
}

type fakeSupervisor struct{}

func (*fakeSupervisor) WithPumps(pumps map[streams.Node]streams.Pump) {

}

func (*fakeSupervisor) Start() error {
	return nil
}

func (*fakeSupervisor) Commit(streams.Processor) error {
	return nil
}

func (*fakeSupervisor) Close() error {
	return nil
}
