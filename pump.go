package streams

import (
	"context"
	"sync"
	"time"

	"github.com/msales/pkg/v3/stats"
)

// Pump represent a Message pump.
type Pump interface {
	sync.Locker

	// Accept takes a message to be processed in the Pump.
	Accept(*Message) error
	// Stop stops the pump.
	Stop()
	// Close closes the pump.
	Close() error
}

// processorPump is an asynchronous Message Pump.
type processorPump struct {
	sync.Mutex

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

	tags := []interface{}{"name", p.name}

	for msg := range p.ch {
		p.pipe.Reset()

		p.Lock()

		start := time.Now()
		err := p.processor.Process(msg)
		if err != nil {
			p.Unlock()
			p.errFn(err)
			return
		}
		latency := time.Since(start) - p.pipe.Duration()

		p.Unlock()

		withStats(msg.Ctx, func(s stats.Stats) {
			s.Timing("node.latency", latency, 0.1, tags...)
			s.Inc("node.throughput", 1, 0.1, tags...)
			s.Gauge("node.back-pressure", pressure(p.ch), 0.1, tags...)
		})
	}
}

// Accept takes a message to be processed in the Pump.
func (p *processorPump) Accept(msg *Message) error {
	p.ch <- msg

	return nil
}

// Stop stops the pump, but does not close it.
func (p *processorPump) Stop() {
	close(p.ch)

	p.wg.Wait()
}

// Close closes the pump.
//
// Stop must be called before closing the pump.
func (p *processorPump) Close() error {
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

	tags := []interface{}{"name", p.name}

	for {
		select {
		case <-p.quit:
			return
		default:
			start := nanotime()

			msg, err := p.source.Consume()
			if err != nil {
				go p.errFn(err)
				return
			}

			if msg.Empty() {
				continue
			}

			latency := time.Duration(nanotime() - start)
			withStats(msg.Ctx, func(s stats.Stats) {
				s.Timing("node.latency", latency, 0.1, tags...)
				s.Inc("node.throughput", 1, 0.1, tags...)
			})

			for _, pump := range p.pumps {
				err = pump.Accept(msg)
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
	}
}
