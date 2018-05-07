package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/msales/streams"
)

type SinkConfig struct {
	sarama.Config

	Brokers []string
	Topic   string

	KeyEncoder   Encoder
	ValueEncoder Encoder

	BatchSize int
}

func NewSinkConfig() *SinkConfig {
	c := &SinkConfig{
		Config: *sarama.NewConfig(),
	}

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
		return sarama.ConfigurationError("Brokers mut have at least one broker")
	case c.KeyEncoder == nil:
		return sarama.ConfigurationError("KeyEncoder must be an instance of Encoder")
	case c.ValueEncoder == nil:
		return sarama.ConfigurationError("ValueEncoder must be an instance of Encoder")
	case c.BatchSize <= 0:
		return sarama.ConfigurationError("BatchSize must be at least 1")
	}

	return nil
}

type Sink struct {
	ctx streams.Context

	keyEncoder   Encoder
	valueEncoder Encoder

	topic    string
	producer sarama.SyncProducer

	batch int
	count int
	buf   []*sarama.ProducerMessage
}

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

// WithContext sets the context on the Processor.
func (p *Sink) WithContext(ctx streams.Context) {
	p.ctx = ctx
}

// Process processes the stream record.
func (p *Sink) Process(key, value interface{}) error {
	k, err := p.keyEncoder.Encode(key)
	if err != nil {
		return err
	}

	v, err := p.valueEncoder.Encode(value)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.ByteEncoder(k),
		Value: sarama.ByteEncoder(v),
	}
	p.buf = append(p.buf, msg)
	p.count++

	if p.count >= p.batch {
		if err := p.producer.SendMessages(p.buf); err != nil {
			return err
		}

		p.count = 0
		p.buf = make([]*sarama.ProducerMessage, 0, p.batch)

		return p.ctx.Commit()
	}

	return nil
}

// Close closes the processor.
func (p *Sink) Close() error {
	return p.producer.Close()
}
