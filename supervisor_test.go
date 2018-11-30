package streams_test

import (
	"testing"

	"github.com/msales/streams"
	"github.com/stretchr/testify/assert"
)

func TestNewSupervisor(t *testing.T) {
	supervisor := streams.NewSupervisor(nil)

	assert.Implements(t, (*streams.Supervisor)(nil), supervisor)
}
