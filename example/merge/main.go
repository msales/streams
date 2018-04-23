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

	stream1 := builder.Source("rand1-source", NewRandIntSource()).
		Filter("filter1", LowNumberFilter)

	builder.Source("rand2-source", NewRandIntSource()).
		Filter("filter2", HighNumberFilter).
		Map("add-hundedred-mapper", AddHundredMapper).
		Merge("merge", stream1).
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

func LowNumberFilter(k, v interface{}) (bool, error) {
	num := v.(int)

	return num < 50, nil
}

func HighNumberFilter(k, v interface{}) (bool, error) {
	num := v.(int)

	return num >= 50, nil
}

func AddHundredMapper(k, v interface{}) (interface{}, interface{}, error) {
	num := v.(int)

	return k, num + 100, nil
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
