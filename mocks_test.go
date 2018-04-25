package streams

import "github.com/stretchr/testify/mock"

type MockNode struct {
	mock.Mock
}

func (mn *MockNode) WithContext(ctx Context) {
	mn.Called(ctx)
}

func (mn *MockNode) AddChild(n Node) {
	mn.Called(n)
}

func (mn *MockNode) Children() []Node {
	args := mn.Called()
	return args.Get(0).([]Node)
}

func (mn *MockNode) Process(key, value interface{}) error {
	args := mn.Called(key, value)
	return args.Error(0)
}

func (mn *MockNode) Close() error {
	args := mn.Called()
	return args.Error(0)
}

type MockProcessor struct {
	mock.Mock
}

func (p *MockProcessor) WithContext(ctx Context) {
	p.Called(ctx)
}

func (p *MockProcessor) Process(key, value interface{}) error {
	args := p.Called(key, value)
	return args.Error(0)
}

func (p *MockProcessor) Close() error {
	args := p.Called()
	return args.Error(0)
}

type MockSource struct {
	mock.Mock
}

func (s *MockSource) Consume() (key, value interface{}, err error) {
	args := s.Called()
	return args.Get(0), args.Get(1), args.Error(2)
}

func (s *MockSource) Commit(sync bool) error {
	args := s.Called(sync)
	return args.Error(0)
}

func (s *MockSource) Close() error {
	args := s.Called()
	return args.Error(0)
}

type MockTask struct {
	mock.Mock
}

func (t *MockTask) Start() {
	t.Called()
}

func (t *MockTask) Commit(sync bool) error {
	args := t.Called(sync)
	return args.Error(0)
}

func (t *MockTask) OnError(fn ErrorFunc) {
	t.Called(fn)
}

func (t *MockTask) Close() {
	t.Called()
}
