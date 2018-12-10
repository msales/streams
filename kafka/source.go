package kafka

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/msales/streams/v2"
	"github.com/pkg/errors"
)

// SourceConfig represents the configuration for a Kafka stream source.
type SourceConfig struct {
	sarama.Config

	Brokers []string
	Topic   string
	GroupID string

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
		return sarama.ConfigurationError("Brokers must have at least one broker")
	case c.KeyDecoder == nil:
		return sarama.ConfigurationError("KeyDecoder must be an instance of Decoder")
	case c.ValueDecoder == nil:
		return sarama.ConfigurationError("ValueDecoder must be an instance of Decoder")
	case c.BufferSize <= 0:
		return sarama.ConfigurationError("BufferSize must be at least 1")
	}

	return nil
}

// Metadata represents an the kafka topic metadata.
type Metadata []*PartitionOffset

func (m Metadata) find(topic string, partition int32) (int, *PartitionOffset) {
	for i, pos := range m {
		if pos.Topic == topic && pos.Partition == partition {
			return i, pos
		}
	}

	return -1, nil
}

// WithOrigin sets the MetadataOrigin on the metadata.
func (m Metadata) WithOrigin(o streams.MetadataOrigin) {
	for _, pos := range m {
		pos.Origin = o
	}
}

// Update updates the given metadata with the contained metadata.
func (m Metadata) Update(v streams.Metadata) streams.Metadata {
	if v == nil {
		return m
	}

	metadata := v.(Metadata)
	for _, newPos := range m {
		i, oldPos := metadata.find(newPos.Topic, newPos.Partition)
		if oldPos == nil {
			metadata = append(metadata, newPos)
			continue
		}

		if newPos.Offset > oldPos.Offset {
			metadata[i] = newPos
		}
	}

	return metadata
}

// Merge merges the contained metadata into the given the metadata.
func (m Metadata) Merge(v streams.Metadata) streams.Metadata {
	if v == nil {
		return m
	}

	metadata := v.(Metadata)
	for _, newPos := range m {
		i, oldPos := metadata.find(newPos.Topic, newPos.Partition)
		if oldPos == nil {
			metadata = append(metadata, newPos)
			continue
		}

		if (newPos.Origin == oldPos.Origin && newPos.Offset < oldPos.Offset) || (newPos.Origin < oldPos.Origin) {
			metadata[i] = newPos
		}
	}

	return metadata
}

// PartitionOffset represents the position in the stream of a message.
type PartitionOffset struct {
	Origin streams.MetadataOrigin

	Topic     string
	Partition int32
	Offset    int64
}

// Source represents a Kafka stream source.
type Source struct {
	topic    string
	consumer *cluster.Consumer

	ctx          context.Context
	keyDecoder   Decoder
	valueDecoder Decoder

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

	consumer, err := cluster.NewConsumer(c.Brokers, c.GroupID, []string{c.Topic}, cc)
	if err != nil {
		return nil, err
	}

	s := &Source{
		topic:        c.Topic,
		consumer:     consumer,
		ctx:          c.Ctx,
		keyDecoder:   c.KeyDecoder,
		valueDecoder: c.ValueDecoder,
		buf:          make(chan *sarama.ConsumerMessage, c.BufferSize),
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

		m := streams.NewMessageWithContext(s.ctx, k, v).
			WithMetadata(s, s.createMetadata(msg))
		return m, nil

	case <-time.After(100 * time.Millisecond):
		return streams.NewMessage(nil, nil), nil
	}
}

// Commit marks the consumed records as processed.
func (s *Source) Commit(v interface{}) error {
	if v == nil {
		return nil
	}

	state := v.(Metadata)
	for _, pos := range state {
		s.consumer.MarkPartitionOffset(pos.Topic, pos.Partition, pos.Offset, "")
	}

	if err := s.consumer.CommitOffsets(); err != nil {
		return errors.Wrap(err, "kafka: could not commit kafka offset")
	}

	return nil
}

// Close closes the Source.
func (s *Source) Close() error {
	return s.consumer.Close()
}

func (s *Source) createMetadata(msg *sarama.ConsumerMessage) Metadata {
	return Metadata{&PartitionOffset{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	}}
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
