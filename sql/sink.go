package sql

import (
	"database/sql"

	"github.com/msales/streams"
)

type TxFunc func(*sql.Tx) error

type InsertFunc func(*sql.Tx, interface{}, interface{}) error

type SinkFunc func(*Sink)

func WithBeginFn(fn TxFunc) SinkFunc {
	return func(s *Sink) {
		s.beginFn = fn
	}
}

func WithCommitFn(fn TxFunc) SinkFunc {
	return func(s *Sink) {
		s.commitFn = fn
	}
}

type Sink struct {
	ctx streams.Context

	db *sql.DB
	tx *sql.Tx

	beginFn  TxFunc
	insertFn InsertFunc
	commitFn TxFunc

	batch int
	count int
}

// NewSink creates a new batch sql insert sink.
func NewSink(db *sql.DB, fn InsertFunc, batch int, opts ...SinkFunc) *Sink {
	s := &Sink{
		db:       db,
		insertFn: fn,
		batch:    batch,
		count:    0,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// WithContext sets the context on the Processor.
func (p *Sink) WithContext(ctx streams.Context) {
	p.ctx = ctx
}

// Process processes the stream record.
func (p *Sink) Process(key, value interface{}) error {
	if err := p.ensureTransaction(); err != nil {
		return err
	}

	if err := p.insertFn(p.tx, key, value); err != nil {
		return err
	}

	p.count++
	if p.count >= p.batch {
		p.count = 0
		return p.commitTransaction()
	}

	return nil
}

func (p *Sink) ensureTransaction() error {
	var err error

	if p.tx != nil {
		return nil
	}

	p.tx, err = p.db.Begin()
	if err != nil {
		return err
	}

	if p.beginFn != nil {
		return p.beginFn(p.tx)
	}

	return nil
}

func (p *Sink) commitTransaction() error {
	if p.tx == nil {
		return nil
	}

	if p.commitFn != nil {
		if err := p.commitFn(p.tx); err != nil {
			return err
		}
	}

	if err := p.tx.Commit(); err != nil {
		p.tx.Rollback()
		return err
	}
	p.tx = nil

	return p.ctx.Commit()
}

// Close closes the processor.
func (p *Sink) Close() error {
	if err := p.commitTransaction(); err != nil {
		return err
	}

	return p.db.Close()
}
