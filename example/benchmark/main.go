package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/msales/pkg/v4/clix"
	"github.com/msales/pkg/v4/stats"
	"github.com/msales/streams/v4"
)

import _ "net/http/pprof"

// BatchSize is the size of commit batches.
const BatchSize = 5000

// Mode is the Task Mode
const Mode = streams.Async

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

	go stats.RuntimeFromContext(ctx, 30*time.Second)

	task, err := task(ctx)
	if err != nil {
		log.Fatal(err.Error())
	}
	task.Start(ctx)
	defer task.Close()

	// Wait for SIGTERM
	<-clix.WaitForSignals()
}

func task(ctx context.Context) (streams.Task, error) {
	builder := streams.NewStreamBuilder()
	builder.Source("nil-source", newNilSource()).
		MapFunc("do-nothing", nothingMapper).
		Process("commit", newCommitProcessor(BatchSize))

	tp, _ := builder.Build()
	task := streams.NewTask(tp, streams.WithMode(Mode))
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
