package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/pkg/errors"
)

type KafkaSource struct {
	consumer *cluster.Consumer

	keyDecoder   Decoder
	valueDecoder Decoder

	buf     chan *sarama.ConsumerMessage
	lastErr error
	lastMsg *sarama.ConsumerMessage
}

func NewKafkaSource(topic, group string, brokers []string, config sarama.Config) (*KafkaSource, error) {
	cc := cluster.NewConfig()
	cc.Config = config
	cc.Consumer.Return.Errors = true

	consumer, err := cluster.NewConsumer(brokers, group, []string{topic}, cc)
	if err != nil {
		return nil, err
	}

	s := &KafkaSource{
		consumer: consumer,
		keyDecoder: ByteDecoder{},
		valueDecoder: ByteDecoder{},
		buf:      make(chan *sarama.ConsumerMessage, 1000),
	}

	go s.readErrors()
	go s.readMessages()

	return s, nil
}

func (s *KafkaSource) WithKeyDecoder(d Decoder) {
	s.keyDecoder = d
}

func (s *KafkaSource) WithValueDecoder(d Decoder) {
	s.valueDecoder = d
}

func (s *KafkaSource) Consume() (key, value interface{}, err error) {
	if s.lastErr != nil {
		return nil, nil, err
	}

	select {
	case msg := <-s.buf:
		s.lastMsg = msg
		k, err := s.keyDecoder.Decode(msg.Key)
		if err != nil {
			return nil, nil ,err
		}

		v, err := s.valueDecoder.Decode(msg.Value)
		if err != nil {
			return nil, nil ,err
		}

		return k, v, nil

	default:
		return nil, nil, nil
	}
}

func (s *KafkaSource) Commit(sync bool) error {
	if s.lastMsg == nil {
		return nil
	}

	s.consumer.MarkOffset(s.lastMsg, "")
	if !sync {
		return nil
	}

	if err := s.consumer.CommitOffsets(); err != nil {
		return errors.Wrap(err, "streams: could not commit kafka offset")
	}

	return nil
}

func (s *KafkaSource) Close() error {
	return s.consumer.Close()
}

func (s *KafkaSource) readErrors() {
	for err := range s.consumer.Errors() {
		s.lastErr = err
	}
}

func (s *KafkaSource) readMessages() {
	for msg := range s.consumer.Messages() {
		s.buf <- msg
	}
}
