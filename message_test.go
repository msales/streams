package streams

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMessage_Empty(t *testing.T) {
	tests := []struct {
		key   interface{}
		value interface{}
		empty bool
	}{
		{"test", "test", false},
		{nil, "test", false},
		{"test", nil, false},
		{nil, nil, true},
	}

	for _, tt := range tests {
		msg := NewMessage(tt.key, tt.value)

		assert.Equal(t, tt.empty, msg.Empty())
	}
}

func TestNewMessage(t *testing.T) {
	msg := NewMessage("test", "test")

	assert.Equal(t, context.Background(), msg.Ctx)
	assert.Equal(t, "test", msg.Key)
	assert.Equal(t, "test", msg.Value)
}

type ctxKey string

func TestNewMessageWithContext(t *testing.T) {
	ctx := context.WithValue(context.Background(), ctxKey("1"), "2")
	msg := NewMessageWithContext(ctx, "test", "test")

	assert.Equal(t, ctx, msg.Ctx)
	assert.Equal(t, "test", msg.Key)
	assert.Equal(t, "test", msg.Value)
}

func TestMessage_Metadata(t *testing.T) {
	msg := NewMessage("test", "test")

	msg.WithMetadata(nil, "test")

	src, meta := msg.Metadata()
	assert.Equal(t, nil, src)
	assert.Equal(t, "test", meta)
}
