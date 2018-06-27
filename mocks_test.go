package streams_test

import (
	"github.com/msales/streams"
	"github.com/stretchr/testify/mock"
)

type MockNode struct {
	mock.Mock
}

func (mn *MockNode) Name() string {
	args := mn.Called()
	return args.String(0)
}

func (mn *MockNode) WithPipe(pipe streams.Pipe) {
	mn.Called(pipe)
}

func (mn *MockNode) AddChild(n streams.Node) {
	mn.Called(n)
}

func (mn *MockNode) Children() []streams.Node {
	args := mn.Called()
	return args.Get(0).([]streams.Node)
}

func (mn *MockNode) Process(msg *streams.Message) error {
	args := mn.Called(msg)
	return args.Error(0)
}

func (mn *MockNode) Close() error {
	args := mn.Called()
	return args.Error(0)
}

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
