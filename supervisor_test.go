package streams_test

import (
	"errors"
	"testing"

	"github.com/msales/streams"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNewSupervisor(t *testing.T) {
	supervisor := streams.NewSupervisor(nil)

	assert.Implements(t, (*streams.Supervisor)(nil), supervisor)
}

func TestSupervisor_Commit(t *testing.T) {
	src1 := source(nil)
	src2 := source(nil)

	comm := committer(nil)
	proc := new(MockProcessor)

	pump1 := pump()
	pump2 := pump()

	meta := map[streams.Processor]streams.Metaitems{
		comm: {
			{Source: src1, Metadata: metadata()},
			{Source: src2, Metadata: metadata()},
		},
		proc: {
			{Source: src1, Metadata: metadata()},
		},
	}

	store := new(MockMetastore)
	store.On("PullAll").Return(meta, nil)
	store.On("Pull", comm).Return(streams.Metaitems{{Source: src1, Metadata: metadata()}}, nil)

	pumps := map[streams.Node]streams.Pump{
		node(comm): pump1,
		node(proc): pump2,
	}

	supervisor := streams.NewSupervisor(store)
	supervisor.WithPumps(pumps)

	err := supervisor.Commit(nil)

	assert.NoError(t, err)
	src1.AssertCalled(t, "Commit", mock.Anything)
	src2.AssertCalled(t, "Commit", mock.Anything)
	store.AssertCalled(t, "Pull", comm)
	pump1.AssertCalled(t, "Lock", mock.Anything)
	pump1.AssertCalled(t, "Unlock", mock.Anything)
	pump2.AssertNotCalled(t, "Lock", mock.Anything)
	pump2.AssertNotCalled(t, "Unlock", mock.Anything)
}

func TestSupervisor_Commit_WithCaller(t *testing.T) {
	src := source(nil)
	comm := committer(nil)
	pump := pump()

	meta := map[streams.Processor]streams.Metaitems{
		comm: {{Source: src, Metadata: metadata()},},
	}

	store := new(MockMetastore)
	store.On("PullAll").Return(meta, nil)
	store.On("Pull", comm).Return(nil, nil)

	pumps := map[streams.Node]streams.Pump{node(comm): pump,}

	supervisor := streams.NewSupervisor(store)
	supervisor.WithPumps(pumps)

	err := supervisor.Commit(comm)

	assert.NoError(t, err)
	src.AssertCalled(t, "Commit", mock.Anything)
	pump.AssertNotCalled(t, "Lock", mock.Anything)
	pump.AssertNotCalled(t, "Unlock", mock.Anything)
}

func TestSupervisor_Commit_PullAllError(t *testing.T) {
	store := new(MockMetastore)
	store.On("PullAll").Return(nil, errors.New("error"))

	supervisor := streams.NewSupervisor(store)

	err := supervisor.Commit(nil)

	assert.Error(t, err)
}

func TestSupervisor_Commit_PullError(t *testing.T) {
	src := source(nil)
	comm := committer(nil)
	pump := pump()

	meta := map[streams.Processor]streams.Metaitems{
		comm: {{Source: src, Metadata: metadata()},},
	}

	store := new(MockMetastore)
	store.On("PullAll").Return(meta, nil)
	store.On("Pull", comm).Return(nil, errors.New("error"))

	pumps := map[streams.Node]streams.Pump{node(comm): pump,}

	supervisor := streams.NewSupervisor(store)
	supervisor.WithPumps(pumps)

	err := supervisor.Commit(nil)

	assert.Error(t, err)
	src.AssertNotCalled(t, "Commit", mock.Anything)
	pump.AssertCalled(t, "Lock", mock.Anything)
	pump.AssertCalled(t, "Unlock", mock.Anything)
}

func TestSupervisor_Commit_UnknownPump(t *testing.T) {
	src := source(nil)
	comm := committer(nil)

	pumps := map[streams.Node]streams.Pump{}
	meta := map[streams.Processor]streams.Metaitems{
		comm: {{Source: src, Metadata: metadata()}},
	}

	store := new(MockMetastore)
	store.On("PullAll").Return(meta, nil)

	supervisor := streams.NewSupervisor(store)
	supervisor.WithPumps(pumps)

	err := supervisor.Commit(nil)

	assert.Error(t, err)
	src.AssertNotCalled(t, "Commit", mock.Anything)
}

func TestSupervisor_Commit_CommitterError(t *testing.T) {
	src := source(nil)
	comm := committer(errors.New("error"))
	pump := pump()

	meta := map[streams.Processor]streams.Metaitems{
		comm: {{Source: src, Metadata: metadata()},},
	}

	store := new(MockMetastore)
	store.On("PullAll").Return(meta, nil)

	pumps := map[streams.Node]streams.Pump{node(comm): pump,}

	supervisor := streams.NewSupervisor(store)
	supervisor.WithPumps(pumps)

	err := supervisor.Commit(nil)

	assert.Error(t, err)
	src.AssertNotCalled(t, "Commit", mock.Anything)
	pump.AssertCalled(t, "Lock", mock.Anything)
	pump.AssertCalled(t, "Unlock", mock.Anything)
}

func source(err error) *MockSource {
	src := new(MockSource)
	src.On("Commit", mock.Anything).Return(err)

	return src
}

func committer(err error) *MockCommitter {
	c := new(MockCommitter)
	c.On("Commit").Return(err)

	return c
}

func node(p streams.Processor) *MockNode {
	n := new(MockNode)
	n.On("Processor").Return(p)

	return n
}

func pump() *MockPump {
	p := new(MockPump)
	p.On("Lock").Return()
	p.On("Unlock").Return()

	return p
}

func metadata() *MockMetadata {
	meta := new(MockMetadata)
	meta.On("Merge", mock.Anything).Return(meta)
	meta.On("Update", mock.Anything).Return(meta)

	return meta
}
