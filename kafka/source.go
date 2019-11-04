package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/msales/streams/v3"
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
	c.Version = sarama.V2_1_0_0

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

// Merge merges the contained metadata into the given the metadata.
func (m Metadata) Merge(v streams.Metadata, s streams.MetadataStrategy) streams.Metadata {
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

		if newPos.Origin > oldPos.Origin {
			continue
		}

		if newPos.Origin < oldPos.Origin {
			metadata[i] = newPos
		}

		// At this point origins are equal
		if (s == streams.Lossless && newPos.Offset < oldPos.Offset) ||
			(s == streams.Dupless && newPos.Offset > oldPos.Offset) {
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
	consumer sarama.ConsumerGroup

	ctx          context.Context
	keyDecoder   Decoder
	valueDecoder Decoder

	buf     chan *sarama.ConsumerMessage
	lastErr error

	session     sarama.ConsumerGroupSession
	sessionLock sync.Mutex
	cancelCtx   func()

	consumerWG sync.WaitGroup
}

// NewSource creates a new Kafka stream source.
func NewSource(c *SourceConfig) (*Source, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	cc := cluster.NewConfig()
	cc.Config = c.Config
	cc.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumerGroup(c.Brokers, c.GroupID, &c.Config)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(c.Ctx)

	s := &Source{
		topic:        c.Topic,
		consumer:     consumer,
		ctx:          ctx,
		keyDecoder:   c.KeyDecoder,
		valueDecoder: c.ValueDecoder,
		buf:          make(chan *sarama.ConsumerMessage, c.BufferSize),
		cancelCtx:    cancel,
	}

	go s.readErrors()
	s.consumerWG.Add(1)
	go s.runConsumer(ctx, c.Topic)

	return s, nil
}

// Consume gets the next record from the Source.
func (s *Source) Consume() (streams.Message, error) {
	if s.lastErr != nil {
		return streams.EmptyMessage, s.lastErr
	}

	select {
	case msg := <-s.buf:
		k, err := s.keyDecoder.Decode(msg.Key)
		if err != nil {
			return streams.EmptyMessage, err
		}

		v, err := s.valueDecoder.Decode(msg.Value)
		if err != nil {
			return streams.EmptyMessage, err
		}

		m := streams.NewMessageWithContext(s.ctx, k, v).
			WithMetadata(s, s.createMetadata(msg))
		return m, nil

	case <-time.After(100 * time.Millisecond):
		return streams.EmptyMessage, nil
	}
}

// Commit marks the consumed records as processed.
func (s *Source) Commit(v interface{}) error {
	if v == nil {
		return nil
	}

	s.sessionLock.Lock()
	defer s.sessionLock.Unlock()

	if s.session == nil {
		return errors.Errorf("kafka: consumer session was closed or doesn't exist")
	}

	state := v.(Metadata)
	for _, pos := range state {
		// This function does not guarantee immediate commit (efficiency reasons). Therefore it is possible
		// that the offsets are never committed if the application crashes. This may lead to double-committing
		// on rare occasions.
		// The result of the "Commit" method on the Committer should be idempotent whenever possible!
		s.session.MarkOffset(pos.Topic, pos.Partition, pos.Offset+1, "")
	}

	return nil
}

// Close closes the Source.
func (s *Source) Close() error {
	s.cancelCtx()
	s.consumerWG.Wait()
	return s.consumer.Close()
}

// Setup is ran once for a new consumer session, before the consumption starts.
func (s *Source) Setup(session sarama.ConsumerGroupSession) error {
	s.sessionLock.Lock()
	defer s.sessionLock.Unlock()

	s.session = session

	return nil
}

// Cleanup is ran once for a session, after the consumption ends.
func (s *Source) Cleanup(_ sarama.ConsumerGroupSession) error {
	s.sessionLock.Lock()
	defer s.sessionLock.Unlock()

	s.session = nil

	return nil
}

// ConsumeClaim consumes messages from a single partition of a topic.
func (s *Source) ConsumeClaim(_ sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		s.buf <- message
	}

	return nil
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

func (s *Source) runConsumer(ctx context.Context, topic string) {
	defer s.consumerWG.Done()
	for {
		err := s.consumer.Consume(ctx, []string{topic}, s)
		if err != nil {
			panic(err)
		}
		if ctx.Err() != nil {
			return
		}
	}
}

// func (s *Source) readMessages() {
// 	for msg := range s.consumer.Messages() {
// 		s.buf <- msg
// 	}
// }
