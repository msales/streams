package sql

import (
	"context"
	"database/sql"
	"errors"

	"github.com/msales/streams/v2"
)

// Transaction represents a SQL transaction handler.
type Transaction interface {
	// Begin handles the start of a SQL transaction.
	Begin(*sql.Tx) error
	// Commit handles the commit of a SQL transaction.
	Commit(*sql.Tx) error
}

// Executor represents a SQL query executor.
type Executor interface {
	// Exec executes a query on the given transaction.
	Exec(*sql.Tx, streams.Message) error
}

// ExecFunc represents a function implementing an Executor.
type ExecFunc func(*sql.Tx, streams.Message) error

// Exec executes a query on the given transaction.
func (fn ExecFunc) Exec(tx *sql.Tx, msg streams.Message) error {
	return fn(tx, msg)
}

// Sink represents a SQL sink processor.
type Sink struct {
	pipe streams.Pipe

	db *sql.DB
	tx *sql.Tx

	exec   Executor
	txHdlr Transaction

	batch int
	count int
}

// NewSink creates a new batch sql insert sink.
func NewSink(db *sql.DB, batch int, exec Executor) (*Sink, error) {
	s := &Sink{
		db:    db,
		exec:  exec,
		batch: batch,
		count: 0,
	}

	if txHdlr, ok := exec.(Transaction); ok {
		s.txHdlr = txHdlr
	}

	if s.batch == 0 {
		return nil, errors.New("sink: batch must be greater then zero")
	}

	return s, nil
}

// WithPipe sets the pipe on the Processor.
func (p *Sink) WithPipe(pipe streams.Pipe) {
	p.pipe = pipe
}

// Process processes the stream record.
func (p *Sink) Process(msg streams.Message) error {
	if err := p.ensureTransaction(); err != nil {
		return err
	}

	if err := p.exec.Exec(p.tx, msg); err != nil {
		return err
	}

	p.count++
	if p.count >= p.batch {
		return p.pipe.Commit(msg)
	}

	return p.pipe.Mark(msg)
}

//Commit commits a processors batch.
func (p *Sink) Commit(ctx context.Context) error {
	p.count = 0

	return p.commitTransaction()
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

	if p.txHdlr != nil {
		return p.txHdlr.Begin(p.tx)
	}

	return nil
}

func (p *Sink) commitTransaction() error {
	if p.tx == nil {
		return nil
	}

	if p.txHdlr != nil {
		if err := p.txHdlr.Commit(p.tx); err != nil {
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
