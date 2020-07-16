package streams

import (
	"sync"
	"time"
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

// Stats represents a stats instance.
type Stats interface {
	// Inc increments a count by the value.
	Inc(name string, value int64, tags ...interface{})
	// Gauge measures the value of a metric.
	Gauge(name string, value float64, tags ...interface{})
	// Timing sends the value of a Duration.
	Timing(name string, value time.Duration, tags ...interface{})
}

type monitor struct {
	stats Stats

	eventWg sync.WaitGroup
	eventCh chan event
	flushWg sync.WaitGroup
	flushCh chan []event
}

// NewMonitor creates a new Monitor.
func NewMonitor(stats Stats, interval time.Duration) Monitor {
	m := &monitor{
		stats:   stats,
		eventCh: make(chan event, 1000),
		flushCh: make(chan []event, 1000),
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
			e.Latency += cached.Latency
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
				m.stats.Timing("node.latency", time.Duration(int64(event.Latency)/event.Count), tags...)
				m.stats.Inc("node.throughput", event.Count, tags...)
				if event.BackPressure >= 0 {
					m.stats.Gauge("node.back-pressure", event.BackPressure, tags...)
				}

			case "commit":
				m.stats.Timing("commit.latency", time.Duration(int64(event.Latency)/event.Count))
				m.stats.Inc("commit.commits", event.Count, 1)
			}
		}

		m.stats.Gauge("monitor.back-pressure", float64(len(m.eventCh))/float64(cap(m.eventCh))*100)
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

type nullStats struct{}

func (s nullStats) Inc(string, int64, ...interface{}) {}

func (s nullStats) Gauge(string, float64, ...interface{}) {}

func (s nullStats) Timing(string, time.Duration, ...interface{}) {}
