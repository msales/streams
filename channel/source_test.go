package channel_test

import (
	"testing"

	"github.com/msales/streams/v2"
	"github.com/msales/streams/v2/channel"
	"github.com/stretchr/testify/assert"
)

func TestNewSource(t *testing.T) {
	src := channel.NewSource(nil)

	assert.Equal(t, &channel.Source{}, src)
}

func TestSource_Consume(t *testing.T) {
	msgs := []*streams.Message{
		{Value: 1},
		{Value: 2},
		{Value: 3},
	}
	ch := make(chan *streams.Message, len(msgs))

	for _, msg := range msgs {
		ch <- msg
	}

	src := channel.NewSource(ch)

	for i := 0; i < len(msgs); i++ {
		msg, err := src.Consume()
		assert.NoError(t, err)
		assert.Equal(t, msgs[i], msg)
	}
}

func TestSource_Consume_WithEmptyMessage(t *testing.T) {
	src := channel.NewSource(nil)

	msg, err := src.Consume()
	assert.NoError(t, err)
	assert.True(t, msg.Empty())
}

func TestSource_Commit(t *testing.T) {
	src := channel.NewSource(nil)

	err := src.Commit(nil)

	assert.NoError(t, err)
}

func TestSource_Close(t *testing.T) {
	ch := make(chan *streams.Message)
	src := channel.NewSource(ch)

	err := src.Close()

	assert.NoError(t, err)
	assert.NotPanics(t, func() { // Assert that the ch is not closed.
		close(ch)
	})
}
