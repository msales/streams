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
	msgs := make([]*streams.Message, 3)
	for i := 0; i < len(msgs); i++ {
		msgs[i] = streams.NewMessage(i, i).WithMetadata(mockSource{}, mockMetadata{})
	}

	ch := make(chan *streams.Message, len(msgs))

	for _, msg := range msgs {
		ch <- msg
	}

	src := channel.NewSource(ch)

	for i := 0; i < len(msgs); i++ {
		msg, err := src.Consume()
		src, meta := msg.Metadata()

		assert.NoError(t, err)
		assert.Equal(t, msgs[i].Key, msg.Key)
		assert.Equal(t, msgs[i].Value, msg.Value)
		assert.Nil(t, src)
		assert.Nil(t, meta)
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

type mockSource struct{}

func (mockSource) Consume() (*streams.Message, error) {
	return nil, nil
}

func (mockSource) Commit(interface{}) error {
	return nil
}

func (mockSource) Close() error {
	return nil
}

type mockMetadata struct{}

func (mockMetadata) WithOrigin(streams.MetadataOrigin) {}

func (m mockMetadata) Merge(streams.Metadata, streams.MetadataStrategy) streams.Metadata {
	return m
}
