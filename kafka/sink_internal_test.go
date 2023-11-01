package kafka

import (
	"errors"
	"testing"

	"github.com/IBM/sarama"
	"github.com/msales/streams/v7"
	"github.com/stretchr/testify/assert"
)

func TestSink_ConsumeReturnsKeyEncodeError(t *testing.T) {
	s := Sink{
		keyEncoder: errorEncoder{},
		buf:        []*sarama.ProducerMessage{},
	}

	err := s.Process(streams.NewMessage("foo", "foo"))

	assert.Error(t, err)
}

func TestSink_ConsumeReturnsValueEncodeError(t *testing.T) {
	s := Sink{
		keyEncoder:   StringEncoder{},
		valueEncoder: errorEncoder{},
		buf:          []*sarama.ProducerMessage{},
	}

	err := s.Process(streams.NewMessage("foo", "foo"))

	assert.Error(t, err)
}

type errorEncoder struct{}

func (errorEncoder) Encode(interface{}) ([]byte, error) {
	return nil, errors.New("test")
}
