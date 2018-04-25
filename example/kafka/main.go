package main

import (
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
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	client, err := stats.NewBufferedStatsd("localhost:8125", "streams.example")
	if err != nil {
		log.Fatal(err.Error())
	}

	p, err := producerTask(client, []string{"127.0.0.1:9092"}, config)
	if err != nil {
		log.Fatal(err.Error())
	}

	c, err := consumerTask(client, []string{"127.0.0.1:9092"}, config)
	if err != nil {
		log.Fatal(err.Error())
	}

	p.Start()
	c.Start()

	// Wait for SIGTERM
	done := listenForSignals()
	<-done

	p.Close()
	c.Close()
}

func producerTask(s stats.Stats, brokers []string, c *sarama.Config) (streams.Task, error) {
	sink, err := kafka.NewKafkaSink("example1", brokers, *c)
	if err != nil {
		return nil, err
	}
	sink.WithValueEncoder(kafka.StringEncoder{})

	builder := streams.NewStreamBuilder()
	builder.Source("rand-source", NewRandIntSource()).
		Map("to-string", StringMapper).
		Process("kafka-sink", sink)

	task := streams.NewTask(builder.Build(), streams.WithStats(s))
	task.OnError(func(err error) {
		log.Fatal(err.Error())
	})

	return task, nil
}

func consumerTask(s stats.Stats, brokers []string, c *sarama.Config) (streams.Task, error) {
	src, err := kafka.NewKafkaSource("example1", "example-consumer", brokers, *c)
	if err != nil {
		return nil, err
	}
	src.WithValueDecoder(kafka.StringDecoder{})

	builder := streams.NewStreamBuilder()
	builder.Source("kafka-source", src).
		Map("to-int", IntMapper)
		// Print("print")

	task := streams.NewTask(builder.Build(), streams.WithStats(s))
	task.OnError(func(err error) {
		log.Fatal(err.Error())
	})

	return task, nil
}

type RandIntSource struct {
	rand *rand.Rand
}

func NewRandIntSource() streams.Source {
	return &RandIntSource{
		rand: rand.New(rand.NewSource(1234)),
	}
}

func (s *RandIntSource) Consume() (key, value interface{}, err error) {
	return nil, s.rand.Intn(100), nil
}

func (s *RandIntSource) Commit(sync bool) error {
	return nil
}

func (s *RandIntSource) Close() error {
	return nil
}

func StringMapper(key, value interface{}) (interface{}, interface{}, error) {
	i := value.(int)

	return key, strconv.Itoa(i), nil
}

func IntMapper(key, value interface{}) (interface{}, interface{}, error) {
	s := value.(string)
	i, err := strconv.Atoi(s)
	if err != nil {
		return nil, nil, err
	}

	return key, i, nil
}

func listenForSignals() chan bool {
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		done <- true
	}()

	return done
}
