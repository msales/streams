package kafka_test

import (
	"context"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"

	"github.com/msales/streams/v6"
	"github.com/msales/streams/v6/kafka"
	"github.com/msales/streams/v6/mocks"
)

func TestNewSinkConfig(t *testing.T) {
	c := kafka.NewSinkConfig()

	assert.IsType(t, &kafka.SinkConfig{}, c)
}

func TestSinkConfig_Validate(t *testing.T) {
	c := kafka.NewSinkConfig()
	c.Brokers = []string{"test"}

	err := c.Validate()

	assert.NoError(t, err)
}

func TestSinkConfig_ValidateErrors(t *testing.T) {
	tests := []struct {
		name string
		cfg  func(*kafka.SinkConfig)
		err  string
	}{
		{
			name: "Brokers",
			cfg: func(c *kafka.SinkConfig) {
				c.Brokers = []string{}
			},
			err: "Brokers must have at least one broker",
		},
		{
			name: "KeyEncoder",
			cfg: func(c *kafka.SinkConfig) {
				c.Brokers = []string{"test"}
				c.KeyEncoder = nil
			},
			err: "KeyEncoder must be an instance of Encoder",
		},
		{
			name: "ValueEncoder",
			cfg: func(c *kafka.SinkConfig) {
				c.Brokers = []string{"test"}
				c.ValueEncoder = nil
			},
			err: "ValueEncoder must be an instance of Encoder",
		},
		{
			name: "BufferSize",
			cfg: func(c *kafka.SinkConfig) {
				c.Brokers = []string{"test"}
				c.BatchSize = 0
			},
			err: "BatchSize must be at least 1",
		},
		{
			name: "BaseConfig",
			cfg: func(c *kafka.SinkConfig) {
				c.Brokers = []string{"test"}
				c.Metadata.Retry.Max = -1
			},
			err: "Metadata.Retry.Max must be >= 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := kafka.NewSinkConfig()
			tt.cfg(c)

			err := c.Validate()

			assert.Equal(t, tt.err, string(err.(sarama.ConfigurationError)))
		})
	}
}

func TestNewSink(t *testing.T) {
	broker0 := sarama.NewMockBroker(t, 0)
	defer broker0.Close()
	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader("test_topic", 0, broker0.BrokerID()),
	})
	c := kafka.NewSinkConfig()
	c.Brokers = []string{broker0.Addr()}
	c.Topic = "test_topic"

	p, err := kafka.NewSink(c)

	assert.NoError(t, err)
	assert.IsType(t, &kafka.Sink{}, p)
}

func TestNewSink_Error(t *testing.T) {
	broker0 := sarama.NewMockBroker(t, 0)
	broker0.Close()
	c := kafka.NewSinkConfig()
	c.Brokers = []string{broker0.Addr()}
	c.Topic = "test_topic"

	_, err := kafka.NewSink(c)

	assert.Error(t, err)
}

func TestNewSink_ValidatesConfig(t *testing.T) {
	c := kafka.NewSinkConfig()

	_, err := kafka.NewSink(c)

	assert.Error(t, err)
}

func TestSink_Process(t *testing.T) {
	broker0 := sarama.NewMockBroker(t, 0)
	defer broker0.Close()
	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader("test_topic", 0, broker0.BrokerID()),
	})

	c := kafka.NewSinkConfig()
	c.Brokers = []string{broker0.Addr()}
	c.Topic = "test_topic"
	c.ValueEncoder = kafka.StringEncoder{}

	p, _ := kafka.NewSink(c)
	defer p.Close()

	pipe := mocks.NewPipe(t)
	pipe.ExpectMark(nil, "foo")
	p.WithPipe(pipe)

	err := p.Process(streams.NewMessage(nil, "foo"))

	assert.NoError(t, err)
}

func TestSink_ProcessCommits(t *testing.T) {
	broker0 := sarama.NewMockBroker(t, 0)
	defer broker0.Close()
	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader("test_topic", 0, broker0.BrokerID()),
	})

	c := kafka.NewSinkConfig()
	c.Brokers = []string{broker0.Addr()}
	c.Topic = "test_topic"
	c.ValueEncoder = kafka.StringEncoder{}
	c.BatchSize = 1

	p, _ := kafka.NewSink(c)
	defer p.Close()

	pipe := mocks.NewPipe(t)
	pipe.ExpectCommit()
	p.WithPipe(pipe)

	err := p.Process(streams.NewMessage(nil, "foo"))

	assert.NoError(t, err)
}

func TestSink_Commit(t *testing.T) {
	broker0 := sarama.NewMockBroker(t, 0)
	defer broker0.Close()
	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader("test_topic", 0, broker0.BrokerID()),
		"ProduceRequest": sarama.NewMockProduceResponse(t).
			SetVersion(2),
	})

	c := kafka.NewSinkConfig()
	c.Brokers = []string{broker0.Addr()}
	c.Topic = "test_topic"
	c.KeyEncoder = kafka.StringEncoder{}
	c.ValueEncoder = kafka.StringEncoder{}
	c.BatchSize = 1

	p, _ := kafka.NewSink(c)
	defer p.Close()

	pipe := mocks.NewPipe(t)
	pipe.ExpectCommit()
	p.WithPipe(pipe)

	p.Process(streams.NewMessage("foo", "foo"))

	err := p.Commit(context.Background())

	assert.NoError(t, err)
}

func TestSink_CommitError(t *testing.T) {
	broker0 := sarama.NewMockBroker(t, 0)
	defer broker0.Close()
	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader("test_topic", 0, broker0.BrokerID()),
		"ProduceRequest": sarama.NewMockProduceResponse(t).
			SetVersion(2).
			SetError("test_topic", 0, sarama.ErrBrokerNotAvailable),
	})

	c := kafka.NewSinkConfig()
	c.Brokers = []string{broker0.Addr()}
	c.Topic = "test_topic"
	c.ValueEncoder = kafka.StringEncoder{}
	c.BatchSize = 1

	p, _ := kafka.NewSink(c)
	defer p.Close()

	pipe := mocks.NewPipe(t)
	pipe.ExpectCommit()
	p.WithPipe(pipe)

	p.Process(streams.NewMessage(nil, "foo"))

	err := p.Commit(context.Background())

	assert.Error(t, err)
}
