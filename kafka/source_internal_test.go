package kafka

import (
	"errors"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
)

func TestSource_ConsumeReturnsLastError(t *testing.T) {
	want := errors.New("test")
	s := Source{
		lastErr: want,
	}

	_, err := s.Consume()

	assert.Equal(t, want, err)
}

func TestSource_ConsumeReturnsKeyDecodeError(t *testing.T) {
	s := Source{
		keyDecoder: errorDecoder{},
		buf:        make(chan *sarama.ConsumerMessage, 1),
	}

	s.buf <- &sarama.ConsumerMessage{
		Key:   []byte(nil),
		Value: []byte("foo"),
	}

	_, err := s.Consume()

	assert.Error(t, err)
}

func TestSource_ConsumeReturnsValueDecodeError(t *testing.T) {
	s := Source{
		keyDecoder:   ByteDecoder{},
		valueDecoder: errorDecoder{},
		buf:          make(chan *sarama.ConsumerMessage, 1),
	}

	s.buf <- &sarama.ConsumerMessage{
		Key:   []byte(nil),
		Value: []byte("foo"),
	}

	_, err := s.Consume()

	assert.Error(t, err)
}

func TestSource_ConsumeTimesOut(t *testing.T) {
	s := Source{
		buf: make(chan *sarama.ConsumerMessage, 1),
	}

	msg, err := s.Consume()

	assert.NoError(t, err)
	assert.Equal(t, nil, msg.Key)
	assert.Equal(t, nil, msg.Value)
}

type errorDecoder struct{}

func (errorDecoder) Decode([]byte) (interface{}, error) {
	return nil, errors.New("test")
}
