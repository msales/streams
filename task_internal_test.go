package streams

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestWithCommitInterval(t *testing.T) {
	task := NewTask(&Topology{sources: map[Source]Node{}}, WithCommitInterval(1))

	assert.IsType(t, &timedSupervisor{}, task.(*streamTask).supervisor)
}

func TestWithMetadataStrategy(t *testing.T) {
	task := NewTask(&Topology{sources: map[Source]Node{}}, WithMetadataStrategy(Dupless))

	assert.IsType(t, &supervisor{}, task.(*streamTask).supervisor)
	assert.Equal(t, Dupless, task.(*streamTask).supervisor.(*supervisor).strategy)
}

func TestWithMonitorInterval(t *testing.T) {
	task := NewTask(&Topology{sources: map[Source]Node{}}, WithMonitorInterval(time.Second))

	assert.Equal(t, time.Second, task.(*streamTask).monitorInterval)
}

func TestWithMonitorInterval_EnforceMinimum(t *testing.T) {
	task := NewTask(&Topology{sources: map[Source]Node{}}, WithMonitorInterval(time.Millisecond))

	assert.Equal(t, 100*time.Millisecond, task.(*streamTask).monitorInterval)
}

func TestWithStats(t *testing.T) {
	stat := new(fakeStats)

	task := NewTask(&Topology{sources: map[Source]Node{}}, WithStats(stat))

	assert.Equal(t, stat, task.(*streamTask).stats)
}

func TestStreamTask_StartSupervisorStartError(t *testing.T) {
	task := &streamTask{
		topology:        &Topology{sources: map[Source]Node{}},
		supervisor:      &fakeSupervisor{StartErr: errors.New("start error")},
		monitorInterval: time.Second,
	}
	defer task.Close()

	err := task.Start(context.Background())

	assert.Error(t, err)
	assert.Equal(t, "start error", err.Error())
}

func TestStreamTask_CloseSupervisorCommitError(t *testing.T) {
	task := &streamTask{
		topology:   &Topology{sources: map[Source]Node{}},
		supervisor: &fakeSupervisor{CommitError: errors.New("commit error")},
	}

	err := task.Close()

	assert.Error(t, err)
	assert.Equal(t, "commit error", err.Error())
}

func TestStreamTask_CloseSupervisorCloseError(t *testing.T) {
	task := &streamTask{
		topology:   &Topology{sources: map[Source]Node{}},
		supervisor: &fakeSupervisor{CloseErr: errors.New("close error")},
	}

	err := task.Close()

	assert.Error(t, err)
	assert.Equal(t, "close error", err.Error())
}

type fakeSupervisor struct {
	StartErr    error
	CommitError error
	CloseErr    error
}

func (*fakeSupervisor) WithContext(context.Context) {}

func (*fakeSupervisor) WithMonitor(Monitor) {}

func (s *fakeSupervisor) WithPumps(map[Node]Pump) {}

func (s *fakeSupervisor) Start() error {
	return s.StartErr
}

func (s *fakeSupervisor) Commit(Processor) error {
	return s.CommitError
}

func (s *fakeSupervisor) Close() error {
	return s.CloseErr
}

type fakeStats struct {
	mock.Mock
}

func (m *fakeStats) Inc(name string, value int64, tags ...interface{}) {
	m.On("Inc", name, value, tags)
}

func (m *fakeStats) Gauge(name string, value float64, tags ...interface{}) {
	m.On("Gauge", name, value, tags)
}

func (m *fakeStats) Timing(name string, value time.Duration, tags ...interface{}) {
	m.On("Timing", name, value, tags)
}
