package channel_test

import (
	"testing"

	"github.com/msales/streams/v5"
	"github.com/msales/streams/v5/channel"
	"github.com/msales/streams/v5/mocks"
	"github.com/stretchr/testify/assert"
)

func TestNewSink(t *testing.T) {
	sink := channel.NewSink(nil, 1)

	assert.IsType(t, &channel.Sink{}, sink)
}

func TestSink_Process(t *testing.T) {
	ch := make(chan streams.Message, 1)
	sink := channel.NewSink(ch, 2)

	pipe := mocks.NewPipe(t)
	pipe.ExpectMark(nil, "test")

	sink.WithPipe(pipe)

	msg := streams.Message{Value: "test"}

	err := sink.Process(msg)

	assert.NoError(t, err)
	assert.Equal(t, msg, <-ch)
	pipe.AssertExpectations()
}

func TestSink_ProcessWithCommit(t *testing.T) {
	ch := make(chan streams.Message, 1)
	sink := channel.NewSink(ch, 1)

	pipe := mocks.NewPipe(t)
	pipe.ExpectCommit()

	sink.WithPipe(pipe)

	msg := streams.Message{Value: "test"}

	err := sink.Process(msg)

	assert.NoError(t, err)
	assert.Equal(t, msg, <-ch)
	pipe.AssertExpectations()
}

func TestSink_Close(t *testing.T) {
	ch := make(chan streams.Message)
	sink := channel.NewSink(ch, 1)

	err := sink.Close()
	_, open := <-ch

	assert.NoError(t, err)
	assert.False(t, open)
}
