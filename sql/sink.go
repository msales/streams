package sql

import (
	"database/sql"
	"errors"
	"time"

	"github.com/msales/streams"
)

// TxFunc represents a function that receives a sql transaction.
type TxFunc func(*sql.Tx) error

// InsertFunc represents a callback to handle processing a Message on the Sink.
type InsertFunc func(*sql.Tx, *streams.Message) error

// SinkFunc represents a function that configures the Sink.
type SinkFunc func(*Sink)

// WithBatchMessages configures the number of messages to send in a batch
// on the Sink.
func WithBatchMessages(messages int) SinkFunc {
	return func(s *Sink) {
		s.batch = messages
	}
}

// WithBatchFrequency configures the frequency to send a batch
// on the Sink.
func WithBatchFrequency(freq time.Duration) SinkFunc {
	return func(s *Sink) {
		s.freq = freq
	}
}

// WithBeginFn sets the transaction start callback on the Sink.
func WithBeginFn(fn TxFunc) SinkFunc {
	return func(s *Sink) {
		s.beginFn = fn
	}
}

// WithCommitFn sets the transaction commit callback on the Sink.
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

	batch      int
	count      int
	freq       time.Duration
	lastCommit time.Time
	lastMsg    *streams.Message
}

// NewSink creates a new batch sql insert sink.
func NewSink(db *sql.DB, fn InsertFunc, opts ...SinkFunc) (*Sink, error) {
	s := &Sink{
		db:       db,
		insertFn: fn,
		batch:    0,
		count:    0,
		freq:     0,
	}

	for _, opt := range opts {
		opt(s)
	}

	if s.batch == 0 && s.freq == 0 {
		return nil, errors.New("sink: neither BatchMessages nor BatchFrequency was set")
	}

	return s, nil
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

	p.lastMsg = msg
	p.count++
	if p.shouldCommit() {
		if err := p.Commit(); err != nil {
			return err
		}

		return p.pipe.Commit(msg)
	}

	return nil
}

//Commit commits a processors batch.
func (p *Sink) Commit() error {
	p.count = 0
	p.lastCommit = time.Now()

	return p.commitTransaction()
}

func (p *Sink) shouldCommit() bool {
	if p.batch > 0 && p.count >= p.batch {
		return true
	}

	if p.freq > 0 && time.Since(p.lastCommit) > p.freq {
		return true
	}

	return false
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

	if p.lastCommit.IsZero() {
		p.lastCommit = time.Now()
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
		_ = p.tx.Rollback()
		return err
	}
	p.tx = nil
	return nil
}

// Close closes the processor.
func (p *Sink) Close() error {
	if p.tx != nil {
		_ = p.tx.Rollback()
	}

	return p.db.Close()
}
