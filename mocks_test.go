package streams_test

import (
	"time"

	"github.com/msales/streams"
	"github.com/stretchr/testify/mock"
)

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

func (mn *MockNode) Processor() (streams.Processor) {
	args := mn.Called()
	return args.Get(0).(streams.Processor)
}

var _ = (streams.Metastore)(&MockMetastore{})

type MockMetastore struct {
	mock.Mock
}

func (s *MockMetastore) Mark(n streams.Node, src streams.Source, meta interface{}) error {
	args := s.Called(n, src, meta)
	return args.Error(0)
}

func (s *MockMetastore) Commit(n streams.Node, src streams.Source, meta interface{}) error {
	args := s.Called(n, src, meta)
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

func (p *MockPump) Process(msg *streams.Message) error {
	args := p.Called(msg)
	return args.Error(0)
}

func (p *MockPump) Close() error {
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
