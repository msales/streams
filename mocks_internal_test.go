package streams

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"
)

var _ = (Metadata)(&MockMetadata{})

type MockMetadata struct {
	mock.Mock
}

func (m *MockMetadata) WithOrigin(o MetadataOrigin) {
	m.Called(o)
}

func (m *MockMetadata) Merge(v Metadata, s MetadataStrategy) Metadata {
	args := m.Called(v, s)
	return args.Get(0).(Metadata)
}

var _ = (Node)(&MockNode{})

type MockNode struct {
	mock.Mock
}

func (mn *MockNode) Name() string {
	args := mn.Called()
	return args.String(0)
}

func (mn *MockNode) AddChild(n Node) {
	mn.Called(n)
}

func (mn *MockNode) Children() []Node {
	args := mn.Called()
	return args.Get(0).([]Node)
}

func (mn *MockNode) Processor() Processor {
	args := mn.Called()
	return args.Get(0).(Processor)
}

var _ = (Monitor)(&MockMonitor{})

type MockMonitor struct {
	mock.Mock
}

func (m *MockMonitor) Processed(name string, l time.Duration, bp float64) {
	m.Called(name, l, bp)
}

func (m *MockMonitor) Committed(l time.Duration) {
	m.Called(l)
}

func (m *MockMonitor) Close() error {
	args := m.Called()

	return args.Error(0)
}

var _ = (Metastore)(&MockMetastore{})

type MockMetastore struct {
	mock.Mock
}

func (s *MockMetastore) Pull(p Processor) (Metaitems, error) {
	args := s.Called(p)

	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(Metaitems), args.Error(1)
}

func (s *MockMetastore) PullAll() (map[Processor]Metaitems, error) {
	args := s.Called()

	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(map[Processor]Metaitems), args.Error(1)
}

func (s *MockMetastore) Mark(p Processor, src Source, meta Metadata) error {
	args := s.Called(p, src, meta)
	return args.Error(0)
}

type MockSupervisor struct {
	mock.Mock
}

func (s *MockSupervisor) Start() error {
	args := s.Called()
	return args.Error(0)
}

func (s *MockSupervisor) Close() error {
	args := s.Called()
	return args.Error(0)
}

func (s *MockSupervisor) WithContext(ctx context.Context) {
	s.Called(ctx)
}

func (s *MockSupervisor) WithMonitor(mon Monitor) {
	s.Called(mon)
}

func (s *MockSupervisor) WithPumps(pumps map[Node]Pump) {
	s.Called(pumps)
}

func (s *MockSupervisor) Commit(p Processor) error {
	args := s.Called(p)
	return args.Error(0)
}

var _ = (TimedPipe)(&MockTimedPipe{})

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

var _ = (Pump)(&MockPump{})

type MockPump struct {
	mock.Mock
}

func (p *MockPump) Lock() {
	p.Called()
}

func (p *MockPump) Unlock() {
	p.Called()
}

func (p *MockPump) Accept(msg Message) error {
	args := p.Called(msg)
	return args.Error(0)
}

func (p *MockPump) Stop() {
	p.Called()
}

func (p *MockPump) Close() error {
	args := p.Called()
	return args.Error(0)
}

func (p *MockPump) WithLock(func() error) error {
	args := p.Called()
	return args.Error(0)
}

var _ = (Processor)(&MockProcessor{})

type MockProcessor struct {
	mock.Mock
}

func (p *MockProcessor) WithPipe(pipe Pipe) {
	p.Called(pipe)
}

func (p *MockProcessor) Process(msg Message) error {
	args := p.Called(msg)
	return args.Error(0)
}

func (p *MockProcessor) Close() error {
	args := p.Called()
	return args.Error(0)
}

var _ = (Processor)(&MockCommitter{})
var _ = (Committer)(&MockCommitter{})

type MockCommitter struct {
	mock.Mock
}

func (p *MockCommitter) WithPipe(pipe Pipe) {
	p.Called(pipe)
}

func (p *MockCommitter) Process(msg Message) error {
	args := p.Called(msg)
	return args.Error(0)
}

func (p *MockCommitter) Commit(ctx context.Context) error {
	args := p.Called(ctx)
	return args.Error(0)
}

func (p *MockCommitter) Close() error {
	args := p.Called()
	return args.Error(0)
}

var _ = (Source)(&MockSource{})

type MockSource struct {
	mock.Mock
}

func (s *MockSource) Consume() (Message, error) {
	args := s.Called()
	return args.Get(0).(Message), args.Error(1)
}

func (s *MockSource) Commit(v interface{}) error {
	args := s.Called(v)
	return args.Error(0)
}

func (s *MockSource) Close() error {
	args := s.Called()
	return args.Error(0)
}

type MockTask struct {
	mock.Mock

	StartCalled   time.Time
	OnErrorCalled time.Time
	CloseCalled   time.Time
}

func (t *MockTask) Start(ctx context.Context) error {
	t.StartCalled = time.Now()

	return t.Called(ctx).Error(0)
}

func (t *MockTask) OnError(fn ErrorFunc) {
	t.OnErrorCalled = time.Now()
	t.Called(fn)
}

func (t *MockTask) Close() error {
	t.CloseCalled = time.Now()

	return t.Called().Error(0)
}

func (t *MockTask) IsRunning() bool {
	return true
}

type ChanSource struct {
	Msgs chan Message
}

func (s *ChanSource) Consume() (Message, error) {
	select {

	case msg := <-s.Msgs:
		return msg, nil

	case <-time.After(time.Millisecond):
		return NewMessage(nil, nil), nil
	}
}

func (s *ChanSource) Commit(v interface{}) error {
	return nil
}

func (s *ChanSource) Close() error {
	close(s.Msgs)

	return nil
}
