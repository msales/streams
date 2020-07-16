package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"

	"github.com/msales/streams/v5"
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
	task.Start(context.Background())
	defer task.Close()

	// Wait for SIGTERM
	waitForShutdown()
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

func oddNumberFilter(msg streams.Message) (bool, error) {
	num := msg.Value.(int)

	return num%2 == 1, nil
}

func doubleMapper(msg streams.Message) (streams.Message, error) {
	num := msg.Value.(int)
	msg.Value = num * 2

	return msg, nil
}

// waitForShutdown blocks until a SIGINT or SIGTERM is received.
func waitForShutdown() {
	quit := make(chan os.Signal)

	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(quit)

	<-quit
}
