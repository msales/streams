package main

import (
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"

	"github.com/msales/streams"
)

func main() {
	builder := streams.NewStreamBuilder()
	builder.Source("rand-source", NewRandIntSource()).
		Filter("odd-filter", OddNumberFilter).
		Map("double-mapper", DoubleMapper).
		Print("print")

	task := streams.NewTask(builder.Build())
	task.OnError(func(err error) {
		log.Fatal(err.Error())
	})
	task.Start()

	// Wait for SIGTERM
	done := listenForSignals()
	<-done

	task.Close()
}

type RandomIntSource struct {
	rand *rand.Rand
}

func NewRandIntSource() streams.Source {
	return &RandomIntSource{
		rand: rand.New(rand.NewSource(1234)),
	}
}

func (s *RandomIntSource) Consume() (*streams.Message, error) {
	return streams.NewMessage(nil, s.rand.Intn(100)), nil
}

func (s *RandomIntSource) Commit(v interface{}) error {
	return nil
}

func (s *RandomIntSource) Close() error {
	return nil
}

func OddNumberFilter(msg *streams.Message) (bool, error) {
	num := msg.Value.(int)

	return num%2 == 1, nil
}

func DoubleMapper(msg *streams.Message) (*streams.Message, error) {
	num := msg.Value.(int)
	msg.Value = num * 2

	return msg, nil
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
