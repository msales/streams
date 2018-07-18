package streams

type Throttler interface {
	Throttle() bool
	Rate() float32
}

type RateThrottler struct {
	rate float32

	counter float32
}

func NewRateThrottler(rate float32) *RateThrottler {
	if rate > 1 {
		panic("the rate must not be greater than 1")
	}

	return &RateThrottler{
		rate: rate,
	}
}

func (t *RateThrottler) Throttle() (should bool) {
	t.counter += t.rate

	should = 1 > t.counter

	if !should {
		t.counter = 0
	}

	return
}

func (t *RateThrottler) Rate() float32 {
	return t.rate
}

type NoopThrottler struct {}

func (NoopThrottler) Throttle() bool {
	return false
}

func (NoopThrottler) Rate() float32 {
	return 1
}
