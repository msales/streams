package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/msales/streams"
)

type KafkaSink struct {
	ctx streams.Context

	keyEncoder   Encoder
	valueEncoder Encoder

	topic    string
	producer sarama.SyncProducer
}

func NewKafkaSink(topic string, brokers []string, c sarama.Config) (*KafkaSink, error) {
	p, err := sarama.NewSyncProducer(brokers, &c)
	if err != nil {
		return nil, err
	}

	return &KafkaSink{
		topic:        topic,
		keyEncoder:   ByteEncoder{},
		valueEncoder: ByteEncoder{},
		producer:     p,
	}, nil
}

// WithContext sets the context on the Processor.
func (p *KafkaSink) WithContext(ctx streams.Context) {
	p.ctx = ctx
}

// WithKeyEncoder sets the Encoder to encode the key with.
func (p *KafkaSink) WithKeyEncoder(e Encoder) {
	p.keyEncoder = e
}

// WithValueEncoder sets the Encoder to encode the value with.
func (p *KafkaSink) WithValueEncoder(e Encoder) {
	p.valueEncoder = e
}

// Process processes the stream record.
func (p *KafkaSink) Process(key, value interface{}) error {
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

	if _, _, err := p.producer.SendMessage(msg); err != nil {
		return err
	}

	return p.ctx.Commit()
}

// Close closes the processor.
func (p *KafkaSink) Close() error {
	return p.producer.Close()
}
