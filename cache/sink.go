package cache

import (
	"time"

	"github.com/msales/pkg/v3/cache"
	"github.com/msales/streams/v4"
)

// Sink represents a Cache streams sink.
type Sink struct {
	pipe streams.Pipe

	cache  cache.Cache
	expire time.Duration

	batch int
	count int
}

// NewSink creates a new cache insert sink.
func NewSink(cache cache.Cache, expire time.Duration, batch int) *Sink {
	return &Sink{
		cache:  cache,
		expire: expire,
		batch:  batch,
	}
}

// WithPipe sets the pipe on the Processor.
func (p *Sink) WithPipe(pipe streams.Pipe) {
	p.pipe = pipe
}

// Process processes the stream record.
func (p *Sink) Process(msg streams.Message) error {
	str := msg.Key.(string)

	if err := p.cache.Set(str, msg.Value, p.expire); err != nil {
		return err
	}

	p.count++
	if p.count >= p.batch {
		p.count = 0
		return p.pipe.Commit(msg)
	}

	return p.pipe.Mark(msg)
}

// Close closes the processor.
func (p *Sink) Close() error {
	return nil
}
