package channel

import (
	"time"

	"github.com/msales/streams/v2"
)

// Compile-time interface check.
var _ streams.Source = (*Source)(nil)

// Source represents a source that consumes messages from a channel.
type Source struct {
	ch chan streams.Message
}

// NewSource creates a new channel Source.
func NewSource(ch chan streams.Message) *Source {
	return &Source{ch: ch}
}

// Consume gets the next record from the Source.
func (s *Source) Consume() (streams.Message, error) {
	select {

	case msg := <-s.ch:
		return msg.WithMetadata(nil, nil), nil

	case <-time.After(100 * time.Millisecond):
		return streams.NewMessage(nil, nil), nil
	}
}

// Commit marks the consumed records as processed.
func (s *Source) Commit(interface{}) error {
	return nil
}

// Close closes the Source.
func (s *Source) Close() error {
	return nil
}
