package streams_test

import (
	"time"

	"github.com/msales/streams"
	"github.com/stretchr/testify/mock"
)

var _ = (streams.Metadata)(&MockMetadata{})

type MockMetadata struct {
	mock.Mock
}

func (m *MockMetadata) Merge(v streams.Metadata) streams.Metadata {
	args := m.Called(v)
	return args.Get(0).(streams.Metadata)
}

var _ = (streams.Node)(&MockNode{})

type MockNode struct {
	mock.Mock
}

func (mn *MockNode) Name() string {
	args := mn.Called()
	return args.String(0)
}

func (mn *MockNode) AddChild(n streams.Node) {
	mn.Called(n)
}

func (mn *MockNode) Children() []streams.Node {
	args := mn.Called()
	return args.Get(0).([]streams.Node)
}

func (mn *MockNode) Processor() streams.Processor {
	args := mn.Called()
	return args.Get(0).(streams.Processor)
}

var _ = (streams.Metastore)(&MockMetastore{})

type MockMetastore struct {
	mock.Mock
}

func (s *MockMetastore) Pull(p streams.Processor) ([]streams.Metaitem, error) {
	args := s.Called(p)

	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]streams.Metaitem), args.Error(1)
}

func (s *MockMetastore) PullAll() (map[streams.Processor][]streams.Metaitem, error) {
	args := s.Called()

	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(map[streams.Processor][]streams.Metaitem), args.Error(1)
}

func (s *MockMetastore) Mark(p streams.Processor, src streams.Source, meta streams.Metadata) error {
	args := s.Called(p, src, meta)
	return args.Error(0)
}

type MockSupervisor struct {
	mock.Mock
}

func (s *MockSupervisor) Commit(p streams.Processor) error {
	args := s.Called(p)
	return args.Error(0)
}

var _ = (streams.TimedPipe)(&MockTimedPipe{})

type MockTimedPipe struct {
	mock.Mock
}

func (p *MockTimedPipe) Reset() {
	p.Called()
}

func (p *MockTimedPipe) Duration() time.Duration {
	args := p.Called()
	return args.Get(0).(time.Duration)
}

var _ = (streams.Pump)(&MockPump{})

type MockPump struct {
	mock.Mock
}

func (p *MockPump) Accept(msg *streams.Message) error {
	args := p.Called(msg)
	return args.Error(0)
}

func (p *MockPump) Close() error {
	args := p.Called()
	return args.Error(0)
}

func (p *MockPump) WithLock(func() error) error {
	args := p.Called()
	return args.Error(0)
}

var _ = (streams.Processor)(&MockProcessor{})

type MockProcessor struct {
	mock.Mock
}

func (p *MockProcessor) WithPipe(pipe streams.Pipe) {
	p.Called(pipe)
}

func (p *MockProcessor) Process(msg *streams.Message) error {
	args := p.Called(msg)
	return args.Error(0)
}

func (p *MockProcessor) Close() error {
	args := p.Called()
	return args.Error(0)
}

var _ = (streams.Processor)(&MockCommitter{})
var _ = (streams.Committer)(&MockCommitter{})

type MockCommitter struct {
	mock.Mock
}

func (p *MockCommitter) WithPipe(pipe streams.Pipe) {
	p.Called(pipe)
}

func (p *MockCommitter) Process(msg *streams.Message) error {
	args := p.Called(msg)
	return args.Error(0)
}

func (p *MockCommitter) Commit() error {
	args := p.Called()
	return args.Error(0)
}

func (p *MockCommitter) Close() error {
	args := p.Called()
	return args.Error(0)
}

var _ = (streams.Source)(&MockSource{})

type MockSource struct {
	mock.Mock
}

func (s *MockSource) Consume() (*streams.Message, error) {
	args := s.Called()
	return args.Get(0).(*streams.Message), args.Error(1)
}

func (s *MockSource) Commit(v interface{}) error {
	args := s.Called(v)
	return args.Error(0)
}

func (s *MockSource) Close() error {
	args := s.Called()
	return args.Error(0)
}
