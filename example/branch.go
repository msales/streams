package main

import (
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"

	"github.com/msales/pkg/stats"
	"github.com/msales/streams"
	"gopkg.in/inconshreveable/log15.v2"
)

func main() {
	logger := log15.New()
	logger.SetHandler(log15.LazyHandler(log15.StreamHandler(os.Stderr, log15.LogfmtFormat())))

	client, err := stats.NewStatsd("localhost:8125", "streams.example")
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	builder := streams.NewStreamBuilder()

	s := builder.Source("rand-source", NewBranchRandIntSource()).
		Branch("branch", BranchEvenNumberFilter, BranchOddNumberFilter)

	// Event numbers
	s[0].Print("print-event")

	// Odd Numbers
	s[1].Map("negative-mapper", NegativeMapper).
		Print("print-negative")

	task := streams.NewTask(builder.Build(), streams.WithStats(client))
	task.OnError(func(err error) {
		log.Fatal(err.Error())
	})
	task.Start()

	// Wait for SIGTERM
	done := listenForSignals()
	<-done

	task.Close()
}

type BranchRandomIntSource struct {
	rand *rand.Rand
}

func NewBranchRandIntSource() streams.Source {
	return &BranchRandomIntSource{
		rand: rand.New(rand.NewSource(1234)),
	}
}

func (s *BranchRandomIntSource) Consume() (key, value interface{}, err error) {
	return nil, s.rand.Intn(100), nil
}

func (s *BranchRandomIntSource) Commit() error {
	return nil
}

func (s *BranchRandomIntSource) Close() error {
	return nil
}

func BranchOddNumberFilter(k, v interface{}) (bool, error) {
	num := v.(int)

	return num%2 == 1, nil
}

func BranchEvenNumberFilter(k, v interface{}) (bool, error) {
	num := v.(int)

	return num%2 == 0, nil
}

func NegativeMapper(k, v interface{}) (interface{}, interface{}, error) {
	num := v.(int)

	return k, num * -1, nil
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
