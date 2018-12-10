package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/msales/streams/v2"
)

// SinkConfig represents the configuration of a Sink.
type SinkConfig struct {
	sarama.Config

	Brokers []string
	Topic   string

	KeyEncoder   Encoder
	ValueEncoder Encoder

	BatchSize int
}

// NewSinkConfig creates a new SinkConfig.
func NewSinkConfig() *SinkConfig {
	c := &SinkConfig{
		Config: *sarama.NewConfig(),
	}

	c.Producer.Return.Successes = true
	c.KeyEncoder = ByteEncoder{}
	c.ValueEncoder = ByteEncoder{}
	c.BatchSize = 1000

	return c
}

// Validate checks a Config instance. It will return a
// sarama.ConfigurationError if the specified values don't make sense.
func (c *SinkConfig) Validate() error {
	if err := c.Config.Validate(); err != nil {
		return err
	}

	switch {
	case c.Brokers == nil || len(c.Brokers) == 0:
		return sarama.ConfigurationError("Brokers must have at least one broker")
	case c.KeyEncoder == nil:
		return sarama.ConfigurationError("KeyEncoder must be an instance of Encoder")
	case c.ValueEncoder == nil:
		return sarama.ConfigurationError("ValueEncoder must be an instance of Encoder")
	case c.BatchSize <= 0:
		return sarama.ConfigurationError("BatchSize must be at least 1")
	}

	return nil
}

// Sink represents a Kafka streams sink.
type Sink struct {
	pipe streams.Pipe

	keyEncoder   Encoder
	valueEncoder Encoder

	topic    string
	producer sarama.SyncProducer

	batch int
	count int
	buf   []*sarama.ProducerMessage
}

// NewSink creates a new Kafka sink.
func NewSink(c *SinkConfig) (*Sink, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	p, err := sarama.NewSyncProducer(c.Brokers, &c.Config)
	if err != nil {
		return nil, err
	}

	s := &Sink{
		topic:        c.Topic,
		keyEncoder:   c.KeyEncoder,
		valueEncoder: c.ValueEncoder,
		producer:     p,
		batch:        c.BatchSize,
		buf:          make([]*sarama.ProducerMessage, 0, c.BatchSize),
	}

	return s, nil
}

// WithPipe sets the pipe on the Processor.
func (p *Sink) WithPipe(pipe streams.Pipe) {
	p.pipe = pipe
}

// Process processes the stream record.
func (p *Sink) Process(msg *streams.Message) error {
	k, err := p.keyEncoder.Encode(msg.Key)
	if err != nil {
		return err
	}

	v, err := p.valueEncoder.Encode(msg.Value)
	if err != nil {
		return err
	}

	var keyEnc sarama.Encoder
	if k != nil {
		keyEnc = sarama.ByteEncoder(k)
	}

	pm := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   keyEnc,
		Value: sarama.ByteEncoder(v),
	}
	p.buf = append(p.buf, pm)
	p.count++

	if p.count >= p.batch {
		return p.pipe.Commit(msg)
	}

	return p.pipe.Mark(msg)
}

//Commit commits a processors batch.
func (p *Sink) Commit() error {
	if err := p.producer.SendMessages(p.buf); err != nil {
		return err
	}

	p.count = 0
	p.buf = p.buf[:0]

	return nil
}

// Close closes the processor.
func (p *Sink) Close() error {
	return p.producer.Close()
}
