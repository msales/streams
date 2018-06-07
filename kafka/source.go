package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/pkg/errors"
)

type SourceConfig struct {
	sarama.Config

	Brokers []string
	Topic   string
	GroupId string

	KeyDecoder   Decoder
	ValueDecoder Decoder

	BufferSize int
}

func NewSourceConfig() *SourceConfig {
	c := &SourceConfig{
		Config: *sarama.NewConfig(),
	}

	c.KeyDecoder = ByteDecoder{}
	c.ValueDecoder = ByteDecoder{}
	c.BufferSize = 1000

	return c
}

// Validate checks a Config instance. It will return a
// sarama.ConfigurationError if the specified values don't make sense.
func (c *SourceConfig) Validate() error {
	if err := c.Config.Validate(); err != nil {
		return err
	}

	switch {
	case c.Brokers == nil || len(c.Brokers) == 0:
		return sarama.ConfigurationError("Brokers mut have at least one broker")
	case c.KeyDecoder == nil:
		return sarama.ConfigurationError("KeyDecoder must be an instance of Decoder")
	case c.ValueDecoder == nil:
		return sarama.ConfigurationError("ValueDecoder must be an instance of Decoder")
	case c.BufferSize <= 0:
		return sarama.ConfigurationError("BufferSize must be at least 1")
	}

	return nil
}

type Source struct {
	consumer *cluster.Consumer

	keyDecoder   Decoder
	valueDecoder Decoder

	state   map[string]map[int32]int64
	buf     chan *sarama.ConsumerMessage
	lastErr error
}

func NewSource(c *SourceConfig) (*Source, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	cc := cluster.NewConfig()
	cc.Config = c.Config
	cc.Consumer.Return.Errors = true

	consumer, err := cluster.NewConsumer(c.Brokers, c.GroupId, []string{c.Topic}, cc)
	if err != nil {
		return nil, err
	}

	s := &Source{
		consumer:     consumer,
		keyDecoder:   c.KeyDecoder,
		valueDecoder: c.ValueDecoder,
		buf:          make(chan *sarama.ConsumerMessage, c.BufferSize),
		state:        make(map[string]map[int32]int64),
	}

	go s.readErrors()
	go s.readMessages()

	return s, nil
}

func (s *Source) Consume() (key, value interface{}, err error) {
	if s.lastErr != nil {
		return nil, nil, err
	}

	select {
	case msg := <-s.buf:
		k, err := s.keyDecoder.Decode(msg.Key)
		if err != nil {
			return nil, nil, err
		}

		v, err := s.valueDecoder.Decode(msg.Value)
		if err != nil {
			return nil, nil, err
		}

		s.markState(msg)

		return k, v, nil

	default:
		return nil, nil, nil
	}
}

func (s *Source) Commit() error {
	for topic, partitions := range s.state {
		for partition, offset := range partitions {
			s.consumer.MarkPartitionOffset(topic, partition, offset, "")
		}
	}

	if err := s.consumer.CommitOffsets(); err != nil {
		return errors.Wrap(err, "streams: could not commit kafka offset")
	}

	return nil
}

func (s *Source) Close() error {
	return s.consumer.Close()
}

func (s *Source) markState(msg *sarama.ConsumerMessage) {
	partitions, ok := s.state[msg.Topic]
	if !ok {
		partitions = make(map[int32]int64)
		s.state[msg.Topic] = partitions
	}

	partitions[msg.Partition] = msg.Offset
}

func (s *Source) readErrors() {
	for err := range s.consumer.Errors() {
		s.lastErr = err
	}
}

func (s *Source) readMessages() {
	for msg := range s.consumer.Messages() {
		s.buf <- msg
	}
}
