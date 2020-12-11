package mocks

import (
	"time"

	"github.com/msales/streams/v6"
)

var _ streams.Source = (*Source)(nil)

// Source is a test source to be used with streams command-level tests.
// It allows consumption of provided set of messages and counts commits.
// Once the counted commits reach expected level an exit signal is emitted.
type Source struct {
	ch       chan streams.Message
	exitCh   chan struct{}
	count    int
	expected int
}

// NewSource creates a new test source.
func NewSource(msgs []streams.Message, expectedCommitCount int) *Source {
	ch := make(chan streams.Message, len(msgs))
	exit := make(chan struct{}, 1)
	for _, msg := range msgs {
		ch <- msg
	}

	return &Source{ch: ch, expected: expectedCommitCount, exitCh: exit}
}

// Consume gets the next record from the Source.
func (s *Source) Consume() (streams.Message, error) {
	select {
	case msg := <-s.ch:
		return msg.WithMetadata(s, nil), nil

	case <-time.After(100 * time.Millisecond):
		return streams.NewMessage(nil, nil), nil
	}
}

// Commit marks the consumed records as processed.
// Once the counted commits reach expected level an exit signal is emitted.
func (s *Source) Commit(interface{}) error {
	s.count++
	if s.count == s.expected {
		s.exitCh <- struct{}{}
		close(s.exitCh)
	}

	return nil
}

// Close closes the Source.
func (s *Source) Close() error {
	return nil
}

// Wait waits until all expected commits are received or a specified timeout occurs.
func (s *Source) Wait(timeout time.Duration) {
	select {
	case <-s.exitCh:
		return
	case <-time.After(timeout):
		return
	}
}
