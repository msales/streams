package main

import (
	"context"
	"log"
	"math/rand"
	"strconv"

	"github.com/Shopify/sarama"
	"github.com/msales/pkg/v3/clix"
	"github.com/msales/pkg/v3/stats"
	"github.com/msales/streams/v2"
	"github.com/msales/streams/v2/kafka"
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

	<-clix.WaitForSignals()
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
		MapFunc("to-string", stringMapper).
		Process("kafka-sink", sink)

	tp, _ := builder.Build()
	task := streams.NewTask(tp)
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
	config.GroupID = "example-consumer"
	config.ValueDecoder = kafka.StringDecoder{}
	config.Ctx = ctx

	src, err := kafka.NewSource(config)
	if err != nil {
		return nil, err
	}

	sink := newCommitProcessor(1000)

	builder := streams.NewStreamBuilder()
	builder.Source("kafka-source", src).
		MapFunc("to-int", intMapper).
		Process("commit-sink", sink)

	tp, _ := builder.Build()
	task := streams.NewTask(tp)
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

type commitProcessor struct {
	pipe streams.Pipe

	batch int
	count int
}

func newCommitProcessor(batch int) streams.Processor {
	return &commitProcessor{
		batch: batch,
	}
}

func (p *commitProcessor) WithPipe(pipe streams.Pipe) {
	p.pipe = pipe
}

func (p *commitProcessor) Process(msg *streams.Message) error {
	p.count++

	if p.count >= p.batch {
		return p.pipe.Commit(msg)
	}

	return p.pipe.Mark(msg)
}

func (p *commitProcessor) Commit() error {
	p.count = 0

	return nil
}

func (p *commitProcessor) Close() error {
	return nil
}
