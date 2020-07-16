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
	"github.com/msales/streams/v5"
	"github.com/msales/streams/v5/kafka"
)

// BatchSize is the size of commit batches.
const BatchSize = 5000

// Mode is the Task Mode
const Mode = streams.Async

func main() {
	ctx := context.Background()

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Version = sarama.V2_1_0_0

	tasks := streams.Tasks{}
	p, err := producerTask([]string{"127.0.0.1:9092"}, config)
	if err != nil {
		log.Fatal(err.Error())
	}
	tasks = append(tasks, p)

	c, err := consumerTask([]string{"127.0.0.1:9092"}, config)
	if err != nil {
		log.Fatal(err.Error())
	}
	tasks = append(tasks, c)

	tasks.Start(ctx)
	defer tasks.Close()

	// Wait for SIGTERM
	waitForShutdown()
}

func producerTask(brokers []string, c *sarama.Config) (streams.Task, error) {
	config := kafka.NewSinkConfig()
	config.Config = *c
	config.Brokers = brokers
	config.Topic = "example1"
	config.ValueEncoder = kafka.StringEncoder{}
	config.BatchSize = BatchSize

	sink, err := kafka.NewSink(config)
	if err != nil {
		return nil, err
	}

	builder := streams.NewStreamBuilder()
	builder.Source("rand-source", newRandIntSource()).
		MapFunc("to-string", stringMapper).
		Process("kafka-sink", sink)

	tp, _ := builder.Build()
	task := streams.NewTask(tp, streams.WithMode(Mode))
	task.OnError(func(err error) {
		log.Fatal(err.Error())
	})

	return task, nil
}

func consumerTask(brokers []string, c *sarama.Config) (streams.Task, error) {
	config := kafka.NewSourceConfig()
	config.Config = *c
	config.Brokers = brokers
	config.Topic = "example1"
	config.GroupID = "example-consumer"
	config.ValueDecoder = kafka.StringDecoder{}

	src, err := kafka.NewSource(config)
	if err != nil {
		return nil, err
	}

	sink := newCommitProcessor(BatchSize)

	builder := streams.NewStreamBuilder()
	builder.Source("kafka-source", src).
		MapFunc("to-int", intMapper).
		Process("commit-sink", sink)

	tp, _ := builder.Build()
	task := streams.NewTask(tp, streams.WithMode(Mode))
	task.OnError(func(err error) {
		log.Fatal(err.Error())
	})

	return task, nil
}

type randIntSource struct {
	rand *rand.Rand
}

func newRandIntSource() streams.Source {
	return &randIntSource{
		rand: rand.New(rand.NewSource(1234)),
	}
}

func (s *randIntSource) Consume() (streams.Message, error) {
	return streams.NewMessage(nil, s.rand.Intn(100)), nil
}

func (s *randIntSource) Commit(v interface{}) error {
	return nil
}

func (s *randIntSource) Close() error {
	return nil
}

func stringMapper(msg streams.Message) (streams.Message, error) {
	i := msg.Value.(int)
	msg.Value = strconv.Itoa(i)

	return msg, nil
}

func intMapper(msg streams.Message) (streams.Message, error) {
	s := msg.Value.(string)
	i, err := strconv.Atoi(s)
	if err != nil {
		return streams.EmptyMessage, err
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

func (p *commitProcessor) Process(msg streams.Message) error {
	p.count++

	if p.count >= p.batch {
		return p.pipe.Commit(msg)
	}

	return p.pipe.Mark(msg)
}

func (p *commitProcessor) Commit(ctx context.Context) error {
	p.count = 0

	return nil
}

func (p *commitProcessor) Close() error {
	return nil
}

// waitForShutdown blocks until a SIGINT or SIGTERM is received.
func waitForShutdown() {
	quit := make(chan os.Signal)

	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(quit)

	<-quit
}
