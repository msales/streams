package cache

import (
	"time"

	"github.com/msales/pkg/cache"
	"github.com/msales/streams"
)

type Sink struct {
	ctx streams.Context

	cache  cache.Cache
	expire time.Duration

	batch int
	count int
}

// NewSink creates a new cache insert sink.
func NewSink(cache cache.Cache, expire time.Duration) *Sink {
	return &Sink{
		cache:  cache,
		expire: expire,
		batch:  1000,
	}
}

// WithContext sets the context on the Processor.
func (p *Sink) WithContext(ctx streams.Context) {
	p.ctx = ctx
}

// Process processes the stream record.
func (p *Sink) Process(msg *streams.Message) error {
	str := msg.Key.(string)

	p.cache.Set(str, msg.Value, p.expire)

	p.count++
	if p.count >= p.batch {
		p.count = 0
		return p.ctx.Commit()
	}

	return nil
}

// Close closes the processor.
func (p *Sink) Close() error {
	return p.ctx.Commit()
}
