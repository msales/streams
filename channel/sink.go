package channel

import "github.com/msales/streams/v2"

// Sink represents a channel sink.
type Sink struct {
	pipe streams.Pipe

	ch chan *streams.Message
}

// NewSink creates a new channel Sink.
func NewSink(ch chan *streams.Message) *Sink {
	return &Sink{ch: ch}
}

// WithPipe sets the pipe on the Processor.
func (s *Sink) WithPipe(pipe streams.Pipe) {
	s.pipe = pipe
}

// Process processes the stream Message.
func (s *Sink) Process(msg *streams.Message) error {
	s.ch <- msg

	return s.pipe.Mark(msg)
}

// Close closes the processor.
func (s *Sink) Close() error {
	close(s.ch)

	return nil
}
