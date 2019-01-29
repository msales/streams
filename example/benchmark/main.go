package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/msales/pkg/v3/stats"
	"github.com/msales/streams/v2"
)

import _ "net/http/pprof"

func main() {
	ctx := context.Background()

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	client, err := stats.NewBufferedStatsd("localhost:8125", "streams.example")
	if err != nil {
		log.Fatal(err.Error())
	}
	ctx = stats.WithStats(ctx, client)

	task, err := task(ctx)
	if err != nil {
		log.Fatal(err.Error())
	}
	task.Start(ctx)
	defer task.Close()

	// Wait for SIGTERM
	<-waitForSignals()
}

func task(ctx context.Context) (streams.Task, error) {
	builder := streams.NewStreamBuilder()
	builder.Source("nil-source", newNilSource()).
		MapFunc("do-nothing", nothingMapper)

	tp, _ := builder.Build()
	task := streams.NewTask(tp)
	task.OnError(func(err error) {
		log.Fatal(err.Error())
	})

	return task, nil
}

type nilSource struct{}

func newNilSource() streams.Source {
	return &nilSource{}
}

func (s *nilSource) Consume() (streams.Message, error) {
	return streams.NewMessage(nil, 1), nil
}

func (s *nilSource) Commit(v interface{}) error {
	return nil
}

func (s *nilSource) Close() error {
	return nil
}

func nothingMapper(msg streams.Message) (streams.Message, error) {
	return streams.EmptyMessage, nil
}

func waitForSignals() chan os.Signal {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	return sigs
}
