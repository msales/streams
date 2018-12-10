package streams

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestWithCommitInterval(t *testing.T) {
	task := NewTask(&Topology{sources: map[Source]Node{}}, WithCommitInterval(1))

	assert.IsType(t, &timedSupervisor{}, task.(*streamTask).supervisor)
}

func TestStreamTask_StartSupervisorStartError(t *testing.T) {
	task := &streamTask{
		topology:   &Topology{sources: map[Source]Node{}},
		supervisor: &fakeSupervisor{StartErr: errors.New("start error")},
	}

	err := task.Start()

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

func (s *fakeSupervisor) Close() error {
	return s.CloseErr
}

func (s *fakeSupervisor) WithPumps(pumps map[Node]Pump) {}

func (s *fakeSupervisor) Start() error {
	return s.StartErr
}

func (s *fakeSupervisor) Commit(Processor) error {
	return s.CommitError
}
