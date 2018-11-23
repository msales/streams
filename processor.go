package streams

import (
	"fmt"
)

// Committer represents a processor that can commit.
type Committer interface {
	//Commit commits a processors batch.
	Commit() error
}

// Processor represents a stream processor.
type Processor interface {
	// WithPipe sets the pipe on the Processor.
	WithPipe(Pipe)
	// Process processes the stream Message.
	Process(*Message) error
	// Close closes the processor.
	Close() error
}

// Mapper represents a mapping function.
type Mapper func(*Message) (*Message, error)

// FlatMapper represents a mapping function that return multiple messages.
type FlatMapper func(*Message) ([]*Message, error)

// Predicate represents a stream filter function.
type Predicate func(*Message) (bool, error)

// BranchProcessor is a processor that branches into one or more streams
//  based on the results of the predicates.
type BranchProcessor struct {
	pipe Pipe
	fns  []Predicate
}

// NewBranchProcessor creates a new BranchProcessor instance.
func NewBranchProcessor(fns []Predicate) Processor {
	return &BranchProcessor{
		fns: fns,
	}
}

// WithPipe sets the pipe on the Processor.
func (p *BranchProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

// Process processes the stream nodeMessage.
func (p *BranchProcessor) Process(msg *Message) error {
	for i, fn := range p.fns {
		ok, err := fn(msg)
		if err != nil {
			return err
		}

		if !ok {
			continue
		}

		if err := p.pipe.ForwardToChild(msg, i); err != nil {
			return err
		}
	}

	return nil
}

// Close closes the processor.
func (p *BranchProcessor) Close() error {
	return nil
}

// FilterProcessor is a processor that filters a stream using a predicate function.
type FilterProcessor struct {
	pipe Pipe
	fn   Predicate
}

// NewFilterProcessor creates a new FilterProcessor instance.
func NewFilterProcessor(fn Predicate) Processor {
	return &FilterProcessor{
		fn: fn,
	}
}

// WithPipe sets the pipe on the Processor.
func (p *FilterProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

// Process processes the stream Message.
func (p *FilterProcessor) Process(msg *Message) error {
	ok, err := p.fn(msg)
	if err != nil {
		return err
	}

	if ok {
		return p.pipe.Forward(msg)
	}

	return p.pipe.Mark(msg)
}

// Close closes the processor.
func (p *FilterProcessor) Close() error {
	return nil
}

// FlatMapProcessor is a processor that maps a stream using a flat mapping function.
type FlatMapProcessor struct {
	pipe Pipe
	fn   FlatMapper
}

// NewFlatMapProcessor creates a new FlatMapProcessor instance.
func NewFlatMapProcessor(fn FlatMapper) Processor {
	return &FlatMapProcessor{
		fn: fn,
	}
}

// WithPipe sets the pipe on the Processor.
func (p *FlatMapProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

// Process processes the stream Message.
func (p *FlatMapProcessor) Process(msg *Message) error {
	msgs, err := p.fn(msg)
	if err != nil {
		return err
	}

	for _, msg := range msgs {
		if err := p.pipe.Forward(msg); err != nil {
			return err
		}
	}

	return nil
}

// Close closes the processor.
func (p *FlatMapProcessor) Close() error {
	return nil
}

// MapProcessor is a processor that maps a stream using a mapping function.
type MapProcessor struct {
	pipe Pipe
	fn   Mapper
}

// NewMapProcessor creates a new MapProcessor instance.
func NewMapProcessor(fn Mapper) Processor {
	return &MapProcessor{
		fn: fn,
	}
}

// WithPipe sets the pipe on the Processor.
func (p *MapProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

// Process processes the stream Message.
func (p *MapProcessor) Process(msg *Message) error {
	msg, err := p.fn(msg)
	if err != nil {
		return err
	}

	return p.pipe.Forward(msg)
}

// Close closes the processor.
func (p *MapProcessor) Close() error {
	return nil
}

// MergeProcessor is a processor that passes the message on,
// keeping track of seen metadata.
type MergeProcessor struct {
	pipe Pipe
}

// NewMergeProcessor creates a new MergeProcessor instance.
func NewMergeProcessor() Processor {
	return &MergeProcessor{}
}

// WithPipe sets the pipe on the Processor.
func (p *MergeProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

// Process processes the stream Message.
func (p *MergeProcessor) Process(msg *Message) error {
	return p.pipe.Forward(msg)
}

// Close closes the processor.
func (p *MergeProcessor) Close() error {
	return nil
}

// PrintProcessor is a processor that prints the stream to stdout.
type PrintProcessor struct {
	pipe Pipe
}

// NewPrintProcessor creates a new PrintProcessor instance.
func NewPrintProcessor() Processor {
	return &PrintProcessor{}
}

// WithPipe sets the pipe on the Processor.
func (p *PrintProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

// Process processes the stream Message.
func (p *PrintProcessor) Process(msg *Message) error {
	fmt.Printf("%v:%v\n", msg.Key, msg.Value)

	return p.pipe.Forward(msg)
}

// Close closes the processor.
func (p *PrintProcessor) Close() error {
	return nil
}
