package main

import (
	"context"
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
	ctx := context.Background()

	logger := log15.New()
	logger.SetHandler(log15.LazyHandler(log15.StreamHandler(os.Stderr, log15.LogfmtFormat())))

	client, err := stats.NewStatsd("localhost:8125", "streams.example")
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	ctx = stats.WithStats(ctx, client)

	builder := streams.NewStreamBuilder()

	s := builder.Source("rand-source", NewRandIntSource(ctx)).
		Branch("branch", BranchEvenNumberFilter, BranchOddNumberFilter)

	// Event numbers
	s[0].Print("print-event")

	// Odd Numbers
	s[1].Map("negative-mapper", NegativeMapper).
		Print("print-negative")

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
	ctx  context.Context
	rand *rand.Rand
}

func NewRandIntSource(ctx context.Context) streams.Source {
	return &RandIntSource{
		ctx:  ctx,
		rand: rand.New(rand.NewSource(1234)),
	}
}

func (s *RandIntSource) Consume() (*streams.Message, error) {
	return streams.NewMessageWithContext(s.ctx, nil, s.rand.Intn(100)), nil
}

func (s *RandIntSource) Commit() error {
	return nil
}

func (s *RandIntSource) Close() error {
	return nil
}

func BranchOddNumberFilter(msg *streams.Message) (bool, error) {
	num := msg.Value.(int)

	return num%2 == 1, nil
}

func BranchEvenNumberFilter(msg *streams.Message) (bool, error) {
	num := msg.Value.(int)

	return num%2 == 0, nil
}

func NegativeMapper(msg *streams.Message) (*streams.Message, error) {
	num := msg.Value.(int)
	msg.Value = num * -1

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
