package streams

import (
	"context"
	"sync"
	"time"

	"github.com/msales/pkg/v3/stats"
)

type event struct {
	EventType    string
	Name         string
	Count        int64
	Latency      time.Duration
	BackPressure float64
}

// Monitor represents a stream event collector.
type Monitor interface {
	// Processed adds a processed event to the Monitor.
	Processed(name string, l time.Duration, bp float64)

	// Committed adds a committed event to the Monitor.
	Committed(l time.Duration)

	// Close closes the monitor.
	Close() error
}

type monitor struct {
	stats stats.Stats

	eventWg sync.WaitGroup
	eventCh chan event
	flushWg sync.WaitGroup
	flushCh chan []event
}

//NewMonitor creates a new Monitor.
func NewMonitor(ctx context.Context, interval time.Duration) Monitor {
	m := &monitor{
		stats:   stats.Null,
		eventCh: make(chan event, 1000),
		flushCh: make(chan []event, 1000),
	}

	if s, ok := stats.FromContext(ctx); ok {
		m.stats = s
	}

	m.eventWg.Add(1)
	go m.runCache(interval)

	m.flushWg.Add(1)
	go m.runFlush()

	return m
}

func (m *monitor) runCache(interval time.Duration) {
	defer m.eventWg.Done()

	var cache []event
	timer := time.NewTicker(interval)
	defer timer.Stop()

	for {
		select {
		case e, ok := <-m.eventCh:
			if !ok {
				if len(cache) > 0 {
					m.flushCh <- cache
				}

				return
			}

			i := eventIndexOf(e, cache)
			if i < 0 {
				cache = append(cache, e)
				continue
			}

			cached := cache[i]
			e.Count += cached.Count
			e.Latency = (cached.Latency + e.Latency) / 2
			cache[i] = e

		case <-timer.C:
			m.flushCh <- cache

			cache = []event{}
		}
	}
}

func eventIndexOf(v event, arr []event) int {
	for i, e := range arr {
		if e.Name == v.Name {
			return i
		}
	}

	return -1
}

func (m *monitor) runFlush() {
	defer m.flushWg.Done()

	for cache := range m.flushCh {
		for _, event := range cache {
			switch event.EventType {
			case "node":
				tags := []interface{}{"name", event.Name}
				_ = m.stats.Timing("node.latency", event.Latency, 1, tags...)
				_ = m.stats.Inc("node.throughput", event.Count, 1, tags...)
				if event.BackPressure >= 0 {
					_ = m.stats.Gauge("node.back-pressure", event.BackPressure, 1, tags...)
				}

			case "commit":
				_ = m.stats.Timing("commit.latency", event.Latency, 1)
				_ = m.stats.Inc("commit.commits", event.Count, 1)
			}
		}

		_ = m.stats.Gauge("monitor.back-pressure", float64(len(m.eventCh))/float64(cap(m.eventCh))*100, 1)
	}
}

// Processed adds a processed event to the monitor.
func (m *monitor) Processed(name string, l time.Duration, bp float64) {
	m.eventCh <- event{
		EventType:    "node",
		Name:         name,
		Count:        1,
		Latency:      l,
		BackPressure: bp,
	}
}

// Committed adds a committed event to the Monitor.
func (m *monitor) Committed(l time.Duration) {
	m.eventCh <- event{
		EventType: "commit",
		Name:      "streams:commit",
		Count:     1,
		Latency:   l,
	}
}

// Close closes the monitor.
func (m *monitor) Close() error {
	close(m.eventCh)

	m.eventWg.Wait()

	close(m.flushCh)

	m.flushWg.Wait()

	return nil
}

type nullMonitor struct{}

func (nullMonitor) Processed(name string, l time.Duration, bp float64) {}

func (nullMonitor) Committed(l time.Duration) {}

func (nullMonitor) Close() error {
	return nil
}
