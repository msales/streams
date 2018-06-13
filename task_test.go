package streams_test

import (
	"testing"

	"github.com/msales/streams"
	"github.com/stretchr/testify/assert"
)

func TestNewTask(t *testing.T) {
	task := streams.NewTask(nil)

	assert.Implements(t, (*streams.Task)(nil), task)
}


