package streams_test

import (
	"errors"
	"github.com/stretchr/testify/mock"
	"testing"

	"github.com/msales/streams"
	"github.com/stretchr/testify/assert"
)

func TestNewSupervisor(t *testing.T) {
	supervisor := streams.NewSupervisor(nil)

	assert.Implements(t, (*streams.Supervisor)(nil), supervisor)
}

func TestSupervisor_Commit(t *testing.T) {
	src1 := source(nil)
	src2 := source(nil)

	comm := new(MockCommitter)
	proc := new(MockProcessor)

	pump1 := pump(nil)
	pump2 := pump(nil)

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

	pumps := map[streams.Node]streams.Pump{
		node(comm): pump1,
		node(proc): pump2,
	}

	supervisor := streams.NewSupervisor(store)
	supervisor.WithPumps(pumps)

	err := supervisor.Commit(nil)

	assert.NoError(t, err)
	src1.(*MockSource).AssertCalled(t, "Commit", mock.Anything)
	src2.(*MockSource).AssertCalled(t, "Commit", mock.Anything)
	pump1.(*MockPump).AssertCalled(t, "WithLock", mock.Anything)
	pump2.(*MockPump).AssertNotCalled(t, "WithLock", mock.Anything)
}

func TestSupervisor_Commit_PullAllError(t *testing.T) {
	store := new(MockMetastore)
	store.On("PullAll").Return(nil, errors.New("error"))

	supervisor := streams.NewSupervisor(store)

	err := supervisor.Commit(nil)

	assert.Error(t, err)
}

func TestSupervisor_Commit_UnknownPumpError(t *testing.T) {
	src := source(nil)
	comm := new(MockCommitter)

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
}

func source(err error) streams.Source {
	src := new(MockSource)
	src.On("Commit", mock.Anything).Return(err)

	return src
}

func node(p streams.Processor) streams.Node {
	n := new(MockNode)
	n.On("Processor").Return(p)

	return n
}

func pump(err error) streams.Pump {
	p := new(MockPump)
	p.On("WithLock", mock.Anything).Return(err)

	return p
}

func metadata() streams.Metadata {
	meta := new(MockMetadata)
	meta.On("Merge", mock.Anything).Return(meta)
	meta.On("Update", mock.Anything).Return(meta)

	return meta
}