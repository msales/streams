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
	Close()
}

type monitor struct {
	stats stats.Stats

	wg      sync.WaitGroup
	eventCh chan event
	flushCh chan []event
}

func NewMonitor(ctx context.Context, interval time.Duration) Monitor {
	m := &monitor{
		stats:   stats.Null,
		eventCh: make(chan event, 1000),
		flushCh: make(chan []event, 1000),
	}

	if s, ok := stats.FromContext(ctx); ok {
		m.stats = s
	}

	m.wg.Add(1)
	go m.runCache(interval)

	go m.runFlush()

	return m
}

func (m *monitor) runCache(interval time.Duration) {
	defer m.wg.Done()

	var cache []event
	lastFlush := nanotime()

	for e := range m.eventCh {
		i := eventIndexOf(e, cache)
		if i < 0 {
			cache = append(cache, e)
			continue
		}

		cached := cache[i]
		e.Count += cached.Count
		e.Latency = (cached.Latency + e.Latency) / 2
		cache[i] = e

		now := nanotime()
		if time.Duration(now-lastFlush) >= interval {
			m.flushCh <- cache

			lastFlush = now
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
func (m *monitor) Close() {
	close(m.eventCh)

	m.wg.Wait()

	close(m.flushCh)
}
