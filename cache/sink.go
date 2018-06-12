package cache

import (
	"context"
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
func (p *Sink) Process(ctx context.Context, k, v interface{}) error {
	str := k.(string)

	p.cache.Set(str, v, p.expire)

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
