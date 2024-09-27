package streams

import (
	"context"
	"fmt"
)

// Committer represents a processor that can commit.
type Committer interface {
	Processor

	// Commit commits a processors batch.
	Commit(ctx context.Context) error
}

// Processor represents a stream processor.
type Processor interface {
	// WithPipe sets the pipe on the Processor.
	WithPipe(Pipe)
	// Process processes the stream Message.
	Process(Message) error
	// Close closes the processor.
	Close() error
}

// Mapper represents a message transformer.
type Mapper interface {
	// Map transforms a message into a new value.
	Map(Message) (Message, error)
}

// FlatMapper represents a transformer that returns zero or many messages.
type FlatMapper interface {
	// FlatMap transforms a message into multiple messages.
	FlatMap(Message) ([]Message, error)
}

// Predicate represents a predicate (boolean-valued function) of a message.
type Predicate interface {
	// Assert tests if the given message satisfies the predicate.
	Assert(Message) (bool, error)
}

var _ = (Mapper)(MapperFunc(nil))

// MapperFunc represents a function implementing the Mapper interface.
type MapperFunc func(Message) (Message, error)

// Map transforms a message into a new value.
func (fn MapperFunc) Map(msg Message) (Message, error) {
	return fn(msg)
}

var _ = (FlatMapper)(FlatMapperFunc(nil))

// FlatMapperFunc represents a function implementing the FlatMapper interface.
type FlatMapperFunc func(Message) ([]Message, error)

// FlatMap transforms a message into multiple messages.
func (fn FlatMapperFunc) FlatMap(msg Message) ([]Message, error) {
	return fn(msg)
}

var _ = (Predicate)(PredicateFunc(nil))

// PredicateFunc represents a function implementing the Predicate interface.
type PredicateFunc func(Message) (bool, error)

// Assert tests if the given message satisfies the predicate.
func (fn PredicateFunc) Assert(msg Message) (bool, error) {
	return fn(msg)
}

// BranchProcessor is a processor that branches into one or more streams
//
//	based on the results of the predicates.
type BranchProcessor struct {
	pipe  Pipe
	preds []Predicate
}

// NewBranchProcessor creates a new BranchProcessor instance.
func NewBranchProcessor(preds []Predicate) Processor {
	return &BranchProcessor{
		preds: preds,
	}
}

// WithPipe sets the pipe on the Processor.
func (p *BranchProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

// Process processes the stream nodeMessage.
func (p *BranchProcessor) Process(msg Message) error {
	for i, pred := range p.preds {
		ok, err := pred.Assert(msg)
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

// FanOutProcessor is a processor that passes the message to multiple children.
type FanOutProcessor struct {
	streams int
	pipe    Pipe
}

// NewFanOutProcessor creates a new FanOutProcessor instance.
func NewFanOutProcessor(streams int) Processor {
	return &FanOutProcessor{
		streams: streams,
	}
}

// WithPipe sets the pipe on the Processor.
func (p *FanOutProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

// Process processes the stream nodeMessage.
func (p *FanOutProcessor) Process(msg Message) error {
	for i := 0; i < p.streams; i++ {
		if err := p.pipe.ForwardToChild(msg, i); err != nil {
			return err
		}
	}

	return nil
}

// Close closes the processor.
func (p *FanOutProcessor) Close() error {
	return nil
}

// FilterProcessor is a processor that filters a stream using a predicate function.
type FilterProcessor struct {
	pipe Pipe
	pred Predicate
}

// NewFilterProcessor creates a new FilterProcessor instance.
func NewFilterProcessor(pred Predicate) Processor {
	return &FilterProcessor{
		pred: pred,
	}
}

// WithPipe sets the pipe on the Processor.
func (p *FilterProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

// Process processes the stream Message.
func (p *FilterProcessor) Process(msg Message) error {
	ok, err := p.pred.Assert(msg)
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
	pipe   Pipe
	mapper FlatMapper
}

// NewFlatMapProcessor creates a new FlatMapProcessor instance.
func NewFlatMapProcessor(mapper FlatMapper) Processor {
	return &FlatMapProcessor{
		mapper: mapper,
	}
}

// WithPipe sets the pipe on the Processor.
func (p *FlatMapProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

// Process processes the stream Message.
func (p *FlatMapProcessor) Process(msg Message) error {
	msgs, err := p.mapper.FlatMap(msg)
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
	pipe   Pipe
	mapper Mapper
}

// NewMapProcessor creates a new MapProcessor instance.
func NewMapProcessor(mapper Mapper) Processor {
	return &MapProcessor{
		mapper: mapper,
	}
}

// WithPipe sets the pipe on the Processor.
func (p *MapProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

// Process processes the stream Message.
func (p *MapProcessor) Process(msg Message) error {
	msg, err := p.mapper.Map(msg)
	if err != nil {
		return err
	}

	return p.pipe.Forward(msg)
}

// Close closes the processor.
func (p *MapProcessor) Close() error {
	return nil
}

// MergeProcessor is a processor that merges multiple streams.
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
func (p *MergeProcessor) Process(msg Message) error {
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
func (p *PrintProcessor) Process(msg Message) error {
	fmt.Printf("%v:%v\n", msg.Key, msg.Value)

	return p.pipe.Forward(msg)
}

// Close closes the processor.
func (p *PrintProcessor) Close() error {
	return nil
}
