package streams

import (
	"sync"
	"time"

	"github.com/msales/pkg/stats"
)

type Pump interface {
	Process(*Message) error
	Close() error
}

var _ = (Pump)(&ProcessorPump{})

type ProcessorPump struct {
	name      string
	processor Processor
	errFunc   ErrorFunc

	ch chan *Message

	wg sync.WaitGroup
}

func NewProcessorPump(node Node, errFunc ErrorFunc) Pump {
	r := &ProcessorPump{
		name:      node.Name(),
		processor: node.Processor(),
		errFunc:   errFunc,
	}

	go r.run()

	return r
}

func (r *ProcessorPump) run() {
	r.wg.Add(1)
	defer r.wg.Done()

	for msg := range r.ch {
		start := time.Now()

		err := r.processor.Process(msg)
		if err != nil {
			r.errFunc(err)
			return
		}

		stats.Timing(msg.Ctx, "node.latency", time.Since(start), 1.0, "name", r.name)
		stats.Inc(msg.Ctx, "node.throughput", 1, 1.0, "name", r.name)
		stats.Gauge(msg.Ctx, "node.back-pressure", pressure(r.ch), 0.1, "name", r.name)
	}
}

func (r *ProcessorPump) Process(msg *Message) error {
	r.ch <- msg

	return nil
}

func (r *ProcessorPump) Close() error {
	close(r.ch)

	r.wg.Wait()

	return r.processor.Close()
}

// pressure calculates how full a channel is.
func pressure(ch chan *Message) float64 {
	l := float64(len(ch))
	c := float64(cap(ch))

	return l / c * 100
}
