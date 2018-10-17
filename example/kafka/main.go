package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/msales/pkg/stats"
	"github.com/msales/streams"
	"github.com/msales/streams/kafka"
)

func main() {
	ctx := context.Background()

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	client, err := stats.NewBufferedStatsd("localhost:8125", "streams.example")
	if err != nil {
		log.Fatal(err.Error())
	}
	ctx = stats.WithStats(ctx, client)

	p, err := producerTask(ctx, []string{"127.0.0.1:9092"}, config)
	if err != nil {
		log.Fatal(err.Error())
	}

	c, err := consumerTask(ctx, []string{"127.0.0.1:9092"}, config)
	if err != nil {
		log.Fatal(err.Error())
	}

	p.Start()
	c.Start()
	defer c.Close()
	defer p.Close()

	// Wait for SIGTERM
	<-waitForSignals()
}

func producerTask(ctx context.Context, brokers []string, c *sarama.Config) (streams.Task, error) {
	config := kafka.NewSinkConfig()
	config.Config = *c
	config.Brokers = brokers
	config.Topic = "example1"
	config.ValueEncoder = kafka.StringEncoder{}

	sink, err := kafka.NewSink(config)
	if err != nil {
		return nil, err
	}

	builder := streams.NewStreamBuilder()
	builder.Source("rand-source", newRandIntSource(ctx)).
		Map("to-string", stringMapper).
		Process("kafka-sink", sink)

	task := streams.NewTask(builder.Build())
	task.OnError(func(err error) {
		log.Fatal(err.Error())
	})

	return task, nil
}

func consumerTask(ctx context.Context, brokers []string, c *sarama.Config) (streams.Task, error) {
	config := kafka.NewSourceConfig()
	config.Config = *c
	config.Brokers = brokers
	config.Topic = "example1"
	config.GroupId = "example-consumer"
	config.ValueDecoder = kafka.StringDecoder{}
	config.Ctx = ctx

	src, err := kafka.NewSource(config)
	if err != nil {
		return nil, err
	}

	builder := streams.NewStreamBuilder()
	builder.Source("kafka-source", src).
		Map("to-int", intMapper).
		Print("print")

	task := streams.NewTask(builder.Build())
	task.OnError(func(err error) {
		log.Fatal(err.Error())
	})

	return task, nil
}

type randIntSource struct {
	ctx  context.Context
	rand *rand.Rand
}

func newRandIntSource(ctx context.Context) streams.Source {
	return &randIntSource{
		ctx:  ctx,
		rand: rand.New(rand.NewSource(1234)),
	}
}

func (s *randIntSource) Consume() (*streams.Message, error) {
	return streams.NewMessageWithContext(s.ctx, nil, s.rand.Intn(100)), nil
}

func (s *randIntSource) Commit(v interface{}) error {
	return nil
}

func (s *randIntSource) Close() error {
	return nil
}

func stringMapper(msg *streams.Message) (*streams.Message, error) {
	i := msg.Value.(int)
	msg.Value = strconv.Itoa(i)

	return msg, nil
}

func intMapper(msg *streams.Message) (*streams.Message, error) {
	s := msg.Value.(string)
	i, err := strconv.Atoi(s)
	if err != nil {
		return nil, err
	}

	msg.Value = i

	return msg, nil
}

func waitForSignals() chan os.Signal {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	return sigs
}
