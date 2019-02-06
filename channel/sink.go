package channel

import "github.com/msales/streams/v3"

// Sink represents a channel sink.
type Sink struct {
	pipe streams.Pipe

	ch chan streams.Message

	batch int
	count int
}

// NewSink creates a new channel Sink.
//
// A batch size of 0 will never commit.
func NewSink(ch chan streams.Message, batch int) *Sink {
	return &Sink{
		ch:    ch,
		batch: batch,
	}
}

// WithPipe sets the pipe on the Processor.
func (s *Sink) WithPipe(pipe streams.Pipe) {
	s.pipe = pipe
}

// Process processes the stream Message.
func (s *Sink) Process(msg streams.Message) error {
	s.ch <- msg

	s.count++
	if s.batch > 0 && s.count >= s.batch {
		s.count = 0
		return s.pipe.Commit(msg)
	}

	return s.pipe.Mark(msg)
}

// Close closes the processor.
func (s *Sink) Close() error {
	close(s.ch)

	return nil
}
