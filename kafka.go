package streams

type KafkaConfig struct {
	Brokers       []string
	ConsumerGroup string
}

type KafkaSource struct {
}

func NewKafkaSource(t string, c *KafkaConfig) Source {
	return &KafkaSource{}
}

func (s *KafkaSource) Consume() (key, value interface{}, err error) {
	return nil, nil, nil
}

func (s *KafkaSource) Commit() error {
	return nil
}

func (s *KafkaSource) Close() error {
	return nil
}
