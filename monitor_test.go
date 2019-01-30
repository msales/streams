package streams_test

import (
	"context"
	"testing"
	"time"

	"github.com/msales/pkg/v3/stats"
	"github.com/msales/streams/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNewMonitor(t *testing.T) {
	mon := streams.NewMonitor(context.Background(), time.Second)
	defer mon.Close()

	assert.Implements(t, (*streams.Monitor)(nil), mon)
}

func TestMonitor_Processed(t *testing.T) {
	stat := new(MockStats)
	stat.On("Inc", "node.throughput", int64(1), float32(1), mock.Anything)
	stat.On("Gauge", "node.back-pressure", float64(50), float32(1), mock.Anything)
	stat.On("Timing", "node.latency", time.Second, float32(1), mock.Anything)

	ctx := stats.WithStats(context.Background(), stat)
	mon := streams.NewMonitor(ctx, time.Millisecond)
	defer mon.Close()

	mon.Processed("test", time.Second, 50)

	time.Sleep(2 * time.Millisecond)

	stat.AssertExpectations(t)
}

func TestMonitor_Committed(t *testing.T) {
	stat := new(MockStats)
	stat.On("Inc", "commit.commits", int64(1), float32(1), mock.Anything)
	stat.On("Timing", "commit.latency", time.Second, float32(1), mock.Anything)

	ctx := stats.WithStats(context.Background(), stat)
	mon := streams.NewMonitor(ctx, time.Millisecond)
	defer mon.Close()

	mon.Committed(time.Second)

	time.Sleep(2 * time.Millisecond)

	stat.AssertExpectations(t)
}

type MockStats struct {
	mock.Mock
}

func (s *MockStats) Dec(name string, value int64, rate float32, tags ...interface{}) error {
	s.Called(name, value, rate, tags)
	return nil
}

func (s *MockStats) Gauge(name string, value float64, rate float32, tags ...interface{}) error {
	s.Called(name, value, rate, tags)
	return nil
}

func (s *MockStats) Inc(name string, value int64, rate float32, tags ...interface{}) error {
	s.Called(name, value, rate, tags)
	return nil
}

func (s *MockStats) Timing(name string, value time.Duration, rate float32, tags ...interface{}) error {
	s.Called(name, value, rate, tags)
	return nil
}

func (s *MockStats) Close() error {
	return nil
}
