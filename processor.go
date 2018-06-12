package streams

import (
	"context"
	"fmt"
)

// Processor represents a stream processor.
type Processor interface {
	// WithContext sets the context on the Processor.
	WithContext(Context)
	// Process processes the stream record.
	Process(context.Context, interface{}, interface{}) error
	// Close closes the processor.
	Close() error
}

// Mapper represents a mapping function
type Mapper func(context.Context, interface{}, interface{}) (context.Context, interface{}, interface{}, error)

// Predicate represents a stream filter function.
type Predicate func(context.Context, interface{}, interface{}) (bool, error)

// BranchProcessor is a processor that branches into one or more streams
//  based on the results of the predicates.
type BranchProcessor struct {
	ctx Context
	fns []Predicate
}

// NewBranchProcessor creates a new BranchProcessor instance.
func NewBranchProcessor(fns []Predicate) Processor {
	return &BranchProcessor{
		fns: fns,
	}
}

// WithContext sets the context on the Processor.
func (p *BranchProcessor) WithContext(ctx Context) {
	p.ctx = ctx
}

// Process processes the stream record.
func (p *BranchProcessor) Process(ctx context.Context, k, v interface{}) error {
	for i, fn := range p.fns {
		ok, err := fn(ctx, k, v)
		if err != nil {
			return err
		}

		if !ok {
			continue
		}

		if err := p.ctx.ForwardToChild(ctx, k, v, i); err != nil {
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
	ctx Context
	fn  Predicate
}

// NewFilterProcessor creates a new FilterProcessor instance.
func NewFilterProcessor(fn Predicate) Processor {
	return &FilterProcessor{
		fn: fn,
	}
}

// WithContext sets the context on the Processor.
func (p *FilterProcessor) WithContext(ctx Context) {
	p.ctx = ctx
}

// Process processes the stream record.
func (p *FilterProcessor) Process(ctx context.Context, k, v interface{}) error {
	ok, err := p.fn(ctx, k, v)
	if err != nil {
		return err
	}

	if ok {
		return p.ctx.Forward(ctx, k, v)
	}
	return nil
}

// Close closes the processor.
func (p *FilterProcessor) Close() error {
	return nil
}

// MapProcessor is a processor that maps a stream using a mapping function.
type MapProcessor struct {
	ctx Context
	fn  Mapper
}

// NewMapProcessor creates a new MapProcessor instance.
func NewMapProcessor(fn Mapper) Processor {
	return &MapProcessor{
		fn: fn,
	}
}

// WithContext sets the context on the Processor.
func (p *MapProcessor) WithContext(ctx Context) {
	p.ctx = ctx
}

// Process processes the stream record.
func (p *MapProcessor) Process(ctx context.Context, k, v interface{}) error {
	ctx, k, v, err := p.fn(ctx, k, v)
	if err != nil {
		return err
	}

	return p.ctx.Forward(ctx, k, v)
}

// Close closes the processor.
func (p *MapProcessor) Close() error {
	return nil
}

// MergeProcessor is a processor that merges multiple streams.
type MergeProcessor struct {
	ctx Context
}

// NewMergeProcessor creates a new MergeProcessor instance.
func NewMergeProcessor() Processor {
	return &MergeProcessor{}
}

// WithContext sets the context on the Processor.
func (p *MergeProcessor) WithContext(ctx Context) {
	p.ctx = ctx
}

// Process processes the stream record.
func (p *MergeProcessor) Process(ctx context.Context, k, v interface{}) error {
	return p.ctx.Forward(ctx, k, v)
}

// Close closes the processor.
func (p *MergeProcessor) Close() error {
	return nil
}

// PrintProcessor is a processor that prints the stream to stdout.
type PrintProcessor struct {
	ctx Context
}

// NewPrintProcessor creates a new PrintProcessor instance.
func NewPrintProcessor() Processor {
	return &PrintProcessor{}
}

// WithContext sets the context on the Processor.
func (p *PrintProcessor) WithContext(ctx Context) {
	p.ctx = ctx
}

// Process processes the stream record.
func (p *PrintProcessor) Process(ctx context.Context, k, v interface{}) error {
	fmt.Printf("%v:%v\n", k, v)

	return p.ctx.Forward(ctx, k, v)
}

// Close closes the processor.
func (p *PrintProcessor) Close() error {
	return nil
}
