package streams

import (
	"fmt"
)

// Processor represents a stream processor.
type Processor interface {
	// WithPipe sets the pipe on the Processor.
	WithPipe(Pipe)
	// Process processes the stream Message.
	Process(*Message) error
	// Close closes the processor.
	Close() error
}

// Mapper represents a mapper.
type Mapper interface {
	Map(*Message) (*Message, error)
}

// FlatMapper represents a mapper that returns multiple messages.
type FlatMapper interface {
	FlatMap(*Message) ([]*Message, error)
}

// Predicate represents a message filter.
type Predicate interface {
	Match(*Message) (bool, error)
}

// MapperFunc represents a mapping function.
type MapperFunc func(*Message) (*Message, error)

// Map maps a message.
func (fn MapperFunc) Map(msg *Message) (*Message, error) {
	return fn(msg)
}

// FlatMapperFunc represents a mapping function that return multiple messages.
type FlatMapperFunc func(*Message) ([]*Message, error)

// FlatMap maps a single message to multiple messages.
func (fn FlatMapperFunc) FlatMap(msg *Message) ([]*Message, error) {
	return fn(msg)
}

// PredicateFunc represents a stream filter function.
type PredicateFunc func(*Message) (bool, error)

// Match matches a message to the predicate.
func (fn PredicateFunc) Match(msg *Message) (bool, error) {
	return fn(msg)
}

// BranchProcessor is a processor that branches into one or more streams
//  based on the results of the predicates.
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
func (p *BranchProcessor) Process(msg *Message) error {
	for i, pred := range p.preds {
		ok, err := pred.Match(msg)
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
func (p *FilterProcessor) Process(msg *Message) error {
	ok, err := p.pred.Match(msg)
	if err != nil {
		return err
	}

	if ok {
		return p.pipe.Forward(msg)
	}
	return nil
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
func (p *FlatMapProcessor) Process(msg *Message) error {
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
func (p *MapProcessor) Process(msg *Message) error {
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

// MergeProcessor is a processor that passes the message on,
// keeping track of seen metadata.
type MergeProcessor struct {
	metadata map[Source]interface{}

	pipe Pipe
}

// NewMergeProcessor creates a new MergeProcessor instance.
func NewMergeProcessor() Processor {
	return &MergeProcessor{
		metadata: map[Source]interface{}{},
	}
}

// WithPipe sets the pipe on the Processor.
func (p *MergeProcessor) WithPipe(pipe Pipe) {
	p.pipe = pipe
}

// Process processes the stream Message.
func (p *MergeProcessor) Process(msg *Message) error {
	// Update the internal metadata state
	for s, v := range msg.Metadata() {
		p.metadata[s] = v
	}

	// Attach metadata to the message
	for s, v := range p.metadata {
		msg.WithMetadata(s, v)
	}

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
