package streams

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
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
