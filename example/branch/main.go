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

	s := builder.Source("rand-source", newRandIntSource(ctx)).
		Branch("branch", branchEvenNumberFilter, branchOddNumberFilter)

	// Event numbers
	s[0].Print("print-event")

	// Odd Numbers
	s[1].Map("negative-mapper", negativeMapper).
		Print("print-negative")

	task := streams.NewTask(builder.Build())
	task.OnError(func(err error) {
		log.Fatal(err.Error())
	})
	task.Start()
	defer task.Close()

	// Wait for SIGTERM
	<-waitForSignals()
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

func branchOddNumberFilter(msg *streams.Message) (bool, error) {
	num := msg.Value.(int)

	return num%2 == 1, nil
}

func branchEvenNumberFilter(msg *streams.Message) (bool, error) {
	num := msg.Value.(int)

	return num%2 == 0, nil
}

func negativeMapper(msg *streams.Message) (*streams.Message, error) {
	num := msg.Value.(int)
	msg.Value = num * -1

	return msg, nil
}

func waitForSignals() chan os.Signal {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	return sigs
}
