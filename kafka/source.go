package kafka

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/msales/streams"
	"github.com/pkg/errors"
)

// SourceConfig represents the configuration for a Kafka stream source.
type SourceConfig struct {
	sarama.Config

	Brokers []string
	Topic   string
	GroupId string

	Ctx          context.Context
	KeyDecoder   Decoder
	ValueDecoder Decoder

	BufferSize int
}

// NewSourceConfig creates a new Kafka source configuration.
func NewSourceConfig() *SourceConfig {
	c := &SourceConfig{
		Config: *sarama.NewConfig(),
	}

	c.Ctx = context.Background()
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

// Source represents a Kafka stream source.
type Source struct {
	consumer *cluster.Consumer

	ctx          context.Context
	keyDecoder   Decoder
	valueDecoder Decoder

	state   map[string]map[int32]int64
	buf     chan *sarama.ConsumerMessage
	lastErr error
}

// NewSource creates a new Kafka stream source.
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
		ctx:          c.Ctx,
		keyDecoder:   c.KeyDecoder,
		valueDecoder: c.ValueDecoder,
		buf:          make(chan *sarama.ConsumerMessage, c.BufferSize),
		state:        make(map[string]map[int32]int64),
	}

	go s.readErrors()
	go s.readMessages()

	return s, nil
}

// Consume gets the next record from the Source.
func (s *Source) Consume() (*streams.Message, error) {
	if s.lastErr != nil {
		return nil, s.lastErr
	}

	select {
	case msg := <-s.buf:
		k, err := s.keyDecoder.Decode(msg.Key)
		if err != nil {
			return nil, err
		}

		v, err := s.valueDecoder.Decode(msg.Value)
		if err != nil {
			return nil, err
		}

		s.markState(msg)

		return streams.NewMessageWithContext(s.ctx, k, v), nil

	case <-time.After(100 * time.Millisecond):
		return streams.NewMessage(nil, nil), nil
	}
}

// Commit marks the consumed records as processed.
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

// Close closes the Source.
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
