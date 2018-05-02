package streams

import (
	"testing"

	"github.com/msales/pkg/log"
	stats2 "github.com/msales/pkg/stats"
	"github.com/stretchr/testify/assert"
)

func TestWithLogger(t *testing.T) {
	logger := log.Null
	task := &streamTask{}

	WithLogger(logger)(task)

	assert.Equal(t, logger, task.logger)
}

func TestWithStats(t *testing.T) {
	stats := stats2.Null

	task := &streamTask{}

	WithStats(stats)(task)

	assert.Equal(t, stats, task.stats)
}
