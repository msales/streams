package streams

import (
	"context"

	"github.com/stretchr/testify/mock"
)

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

func (mn *MockNode) Process(ctx context.Context, k, v interface{}) error {
	args := mn.Called(ctx, k, v)
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

func (p *MockProcessor) Process(ctx context.Context, k, v interface{}) error {
	args := p.Called(ctx, k, v)
	return args.Error(0)
}

func (p *MockProcessor) Close() error {
	args := p.Called()
	return args.Error(0)
}

type MockSource struct {
	mock.Mock
}

func (s *MockSource) Consume() (ctx context.Context, k, v interface{}, err error) {
	args := s.Called()
	return args.Get(0).(context.Context), args.Get(1), args.Get(2), args.Error(3)
}

func (s *MockSource) Commit() error {
	args := s.Called()
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

func (t *MockTask) Commit() error {
	args := t.Called()
	return args.Error(0)
}

func (t *MockTask) OnError(fn ErrorFunc) {
	t.Called(fn)
}

func (t *MockTask) Close() error {
	args := t.Called()
	return args.Error(0)
}
