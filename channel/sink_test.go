package channel_test

import (
	"testing"

	"github.com/msales/streams/v2"
	"github.com/msales/streams/v2/channel"
	"github.com/msales/streams/v2/mocks"
	"github.com/stretchr/testify/assert"
)

func TestNewSink(t *testing.T) {
	sink := channel.NewSink(nil)

	assert.Equal(t, &channel.Sink{}, sink)
}

func TestSink_Close(t *testing.T) {
	ch := make(chan *streams.Message)
	sink := channel.NewSink(ch)

	err := sink.Close()
	_, open := <-ch

	assert.NoError(t, err)
	assert.False(t, open)
}

func TestSink_Process(t *testing.T) {
	ch := make(chan *streams.Message, 1)
	sink := channel.NewSink(ch)

	pipe := mocks.NewPipe(t)
	pipe.ExpectMark(nil, "test")

	sink.WithPipe(pipe)

	msg := &streams.Message{Value: "test"}

	err := sink.Process(msg)

	assert.NoError(t, err)
	assert.Equal(t, msg, <-ch)
	pipe.AssertExpectations()
}
