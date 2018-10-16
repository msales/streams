package streams

import (
	"context"
	"sync"
	"time"

	"github.com/msales/pkg/stats"
)

// Pump represent a Message pump.
type Pump interface {
	// Process processes a message in the Pump.
	Process(*Message) error
	// Close closes the pump.
	Close() error
}

// processorPump is an asynchronous Message Pump.
type processorPump struct {
	name      string
	processor Processor
	pipe      TimedPipe
	errFn     ErrorFunc

	ch chan *Message

	wg sync.WaitGroup
}

// NewPump creates a new processorPump instance.
func NewPump(node Node, pipe TimedPipe, errFn ErrorFunc) Pump {
	p := &processorPump{
		name:      node.Name(),
		processor: node.Processor(),
		pipe:      pipe,
		errFn:     errFn,
		ch:        make(chan *Message, 1000),
	}

	go p.run()

	return p
}

func (p *processorPump) run() {
	p.wg.Add(1)
	defer p.wg.Done()

	for msg := range p.ch {
		p.pipe.Reset()
		start := time.Now()

		if err := p.processor.Process(msg); err != nil {
			p.errFn(err)
			return
		}

		latency := time.Since(start) - p.pipe.Duration()
		withStats(msg.Ctx, func(s stats.Stats) {
			s.Timing("node.latency", latency, 0.1, "name", p.name)
			s.Inc("node.throughput", 1, 0.1, "name", p.name)
			s.Gauge("node.back-pressure", pressure(p.ch), 0.1, "name", p.name)
		})
	}
}

// Process processes a message in the Pump.
func (p *processorPump) Process(msg *Message) error {
	p.ch <- msg

	return nil
}

// Close closes the pump.
func (p *processorPump) Close() error {
	close(p.ch)

	p.wg.Wait()

	return p.processor.Close()
}

// pressure calculates how full a channel is.
func pressure(ch chan *Message) float64 {
	l := float64(len(ch))
	c := float64(cap(ch))

	return l / c * 100
}

// SourcePump represents a Message pump for sources.
type SourcePump interface {
	// Stop stops the source pump from running.
	Stop()
	// Close closed the source pump.
	Close() error
}

// SourcePumps represents a set of source pumps.
type SourcePumps []SourcePump

// StopAll stops all source pumps.
func (p SourcePumps) StopAll() {
	for _, sp := range p {
		sp.Stop()
	}
}

// sourcePump represents a Message pump for sources.
type sourcePump struct {
	name   string
	source Source
	pumps  []Pump
	errFn  ErrorFunc

	quit chan struct{}
	wg   sync.WaitGroup
}

// NewSourcePump creates a new SourcePump.
func NewSourcePump(name string, source Source, pumps []Pump, errFn ErrorFunc) SourcePump {
	p := &sourcePump{
		name:   name,
		source: source,
		pumps:  pumps,
		errFn:  errFn,
		quit:   make(chan struct{}, 2),
	}

	go p.run()

	return p
}

func (p *sourcePump) run() {
	p.wg.Add(1)
	defer p.wg.Done()

	for {
		select {
		case <-p.quit:
			return
		default:
			start := time.Now()

			msg, err := p.source.Consume()
			if err != nil {
				go p.errFn(err)
				return
			}

			if msg.Empty() {
				continue
			}

			latency := time.Since(start)
			withStats(msg.Ctx, func(s stats.Stats) {
				s.Timing("node.latency", latency, 0.1, "name", p.name)
				s.Inc("node.throughput", 1, 0.1, "name", p.name)
			})

			for _, pump := range p.pumps {
				err = pump.Process(msg)
				if err != nil {
					go p.errFn(err)
					return
				}
			}
		}
	}
}

// Stop stops the source pump from running.
func (p *sourcePump) Stop() {
	p.quit <- struct{}{}

	p.wg.Wait()
}

// Close closes the source pump.
func (p *sourcePump) Close() error {
	close(p.quit)

	return p.source.Close()
}

func withStats(ctx context.Context, fn func(s stats.Stats)) {
	if s, ok := stats.FromContext(ctx); ok {
		fn(s)
		return
	}
	fn(stats.Null)
}
