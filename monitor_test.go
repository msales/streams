package streams_test

import (
	"testing"
	"time"

	"github.com/msales/streams/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNewMonitor(t *testing.T) {
	mon := streams.NewMonitor(new(MockStats), time.Second)
	defer mon.Close()

	assert.Implements(t, (*streams.Monitor)(nil), mon)
}

func TestMonitor_Processed(t *testing.T) {
	stat := new(MockStats)
	stat.On("Inc", "node.throughput", int64(1), mock.Anything)
	stat.On("Gauge", "node.back-pressure", float64(50), mock.Anything)
	stat.On("Timing", "node.latency", time.Second, mock.Anything)
	stat.On("Gauge", "monitor.back-pressure", mock.Anything, mock.Anything)

	mon := streams.NewMonitor(stat, time.Microsecond)

	mon.Processed("test", time.Second, 50)

	time.Sleep(3 * time.Microsecond)

	_ = mon.Close()

	stat.AssertExpectations(t)
}

func TestMonitor_Committed(t *testing.T) {
	stat := new(MockStats)
	stat.On("Inc", "commit.commits", int64(1), mock.Anything)
	stat.On("Timing", "commit.latency", time.Second, mock.Anything)
	stat.On("Gauge", "monitor.back-pressure", mock.Anything, mock.Anything)

	mon := streams.NewMonitor(stat, time.Microsecond)

	mon.Committed(time.Second)

	time.Sleep(3 * time.Microsecond)

	_ = mon.Close()

	stat.AssertExpectations(t)
}

type MockStats struct {
	mock.Mock
}

func (s *MockStats) Gauge(name string, value float64, tags ...interface{}) {
	s.Called(name, value, tags)
}

func (s *MockStats) Inc(name string, value int64, tags ...interface{}) {
	s.Called(name, value, tags)
}

func (s *MockStats) Timing(name string, value time.Duration, tags ...interface{}) {
	s.Called(name, value, tags)
}
