package streams_test

import (
	"testing"
	"github.com/msales/streams"
	"github.com/stretchr/testify/assert"
)

func TestNewRateThrottler(t *testing.T) {
	throttler := streams.NewRateThrottler(0.5)

	assert.IsType(t, &streams.RateThrottler{}, throttler)
}

func TestRateThrottler_Throttle(t *testing.T) {
	throttler := streams.NewRateThrottler(0.5)

	should := throttler.Throttle()
	assert.True(t, should)

	should = throttler.Throttle()
	assert.False(t, should)
}

func TestRateThrottler_Rate(t *testing.T) {
	throttler := streams.NewRateThrottler(0.5)

	assert.Equal(t, float32(0.5), throttler.Rate())
}

func TestNoopThrottler_Throttle(t *testing.T) {
	throttler := streams.NoopThrottler{}

	should := throttler.Throttle()

	assert.False(t, should)
}

func TestNoopThrottler_Rate(t *testing.T) {
	throttler := streams.NoopThrottler{}

	assert.Equal(t, float32(1), throttler.Rate())
}
