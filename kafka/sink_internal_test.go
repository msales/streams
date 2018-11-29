package kafka

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/msales/streams"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestSink_ConsumeReturnsKeyEncodeError(t *testing.T) {
	s := Sink{
		keyEncoder: errorEncoder{},
		buf: []*sarama.ProducerMessage{},
	}

	err := s.Process(streams.NewMessage("foo", "foo"))

	assert.Error(t, err)
}

func TestSink_ConsumeReturnsValueEncodeError(t *testing.T) {
	s := Sink{
		keyEncoder: StringEncoder{},
		valueEncoder: errorEncoder{},
		buf: []*sarama.ProducerMessage{},
	}

	err := s.Process(streams.NewMessage("foo", "foo"))

	assert.Error(t, err)
}

type errorEncoder struct {}

func (errorEncoder) Encode(interface{}) ([]byte, error) {
	return nil, errors.New("test")
}

