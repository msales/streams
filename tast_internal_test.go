package streams

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCommitInterval(t *testing.T) {
	task := &streamTask{}
	fn := CommitInterval(1)

	fn(task)

	assert.IsType(t, &timedSupervisor{}, task.supervisor)
}
