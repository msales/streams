package main

import (
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"

	"github.com/msales/streams/v2"
)

func main() {
	builder := streams.NewStreamBuilder()
	builder.Source("rand-source", newRandIntSource()).
		FilterFunc("odd-filter", oddNumberFilter).
		MapFunc("double-mapper", doubleMapper).
		Print("print")

	tp, _ := builder.Build()
	task := streams.NewTask(tp)
	task.OnError(func(err error) {
		log.Fatal(err.Error())
	})
	task.Start()
	defer task.Close()

	// Wait for SIGTERM
	<-waitForSignals()
}

type randIntSource struct {
	rand *rand.Rand
}

func newRandIntSource() streams.Source {
	return &randIntSource{
		rand: rand.New(rand.NewSource(1234)),
	}
}

func (s *randIntSource) Consume() (*streams.Message, error) {
	return streams.NewMessage(nil, s.rand.Intn(100)), nil
}

func (s *randIntSource) Commit(v interface{}) error {
	return nil
}

func (s *randIntSource) Close() error {
	return nil
}

func oddNumberFilter(msg *streams.Message) (bool, error) {
	num := msg.Value.(int)

	return num%2 == 1, nil
}

func doubleMapper(msg *streams.Message) (*streams.Message, error) {
	num := msg.Value.(int)
	msg.Value = num * 2

	return msg, nil
}

func waitForSignals() chan os.Signal {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	return sigs
}
