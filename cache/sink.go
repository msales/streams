package cache

import (
	"time"

	"github.com/msales/pkg/cache"
	"github.com/msales/streams"
)

type CacheSink struct {
	ctx streams.Context

	cache  cache.Cache
	expire time.Duration
}

// NewCacheSink creates a new cache insert sink.
func NewCacheSink(cache cache.Cache, expire time.Duration) *CacheSink {
	return &CacheSink{
		cache:  cache,
		expire: expire,
	}
}

// WithContext sets the context on the Processor.
func (p *CacheSink) WithContext(ctx streams.Context) {
	p.ctx = ctx
}

// Process processes the stream record.
func (p *CacheSink) Process(key, value interface{}) error {
	k := key.(string)

	p.cache.Set(k, value, p.expire)
	p.ctx.CommitAsync()

	return nil
}

// Close closes the processor.
func (p *CacheSink) Close() error {
	return p.ctx.Commit()
}
