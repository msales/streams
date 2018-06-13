package sql

import (
	"database/sql"

	"github.com/msales/streams"
)

// TxFunc represents a function that receives a sql transaction.
type TxFunc func(*sql.Tx) error

type InsertFunc func(*sql.Tx, *streams.Message) error

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

// Sink represents a SQL sink processor.
type Sink struct {
	pipe streams.Pipe

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

// WithPipe sets the pipe on the Processor.
func (p *Sink) WithPipe(pipe streams.Pipe) {
	p.pipe = pipe
}

// Process processes the stream record.
func (p *Sink) Process(msg *streams.Message) error {
	if err := p.ensureTransaction(); err != nil {
		return err
	}

	if err := p.insertFn(p.tx, msg); err != nil {
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

	return p.pipe.Commit()
}

// Close closes the processor.
func (p *Sink) Close() error {
	if err := p.commitTransaction(); err != nil {
		return err
	}

	return p.db.Close()
}
