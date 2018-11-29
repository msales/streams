package streams

import (
	"context"
	"sync"
	"time"

	"github.com/msales/pkg/v3/stats"
)

// Pump represent a Message pump.
type Pump interface {
	// Accept takes a message to be processed in the Pump.
	Accept(*Message) error

	// Close closes the pump.
	Close() error

	// With lock executes a function after the pump is safely paused.
	WithLock(func() error) error
}

// processorPump is an asynchronous Message Pump.
type processorPump struct {
	name      string
	processor Processor
	pipe      TimedPipe
	errFn     ErrorFunc

	ch chan *Message

	lock sync.Mutex
	wg   sync.WaitGroup
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

		p.lock.Lock()

		start := time.Now()
		err := p.processor.Process(msg)
		if err != nil {
			p.lock.Unlock()
			p.errFn(err)
			return
		}
		latency := time.Since(start) - p.pipe.Duration()

		p.lock.Unlock()

		withStats(msg.Ctx, func(s stats.Stats) {
			s.Timing("node.latency", latency, 0.1, tags...)
			s.Inc("node.throughput", 1, 0.1, tags...)
			s.Gauge("node.back-pressure", pressure(p.ch), 0.1, tags...)
		})
	}
}

func (p *processorPump) WithLock(fn func() error) error {
	p.lock.Lock()
	err := fn()
	p.lock.Unlock()

	return err
}

// Accept takes a message to be processed in the Pump.
func (p *processorPump) Accept(msg *Message) error {
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

	tags := []interface{}{"name", p.name}

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
