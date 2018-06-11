package streams

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithContext(t *testing.T) {
	ctx := context.Background()
	task := &streamTask{}

	WithContext(ctx)(task)

	assert.Equal(t, ctx, task.ctx)
}
