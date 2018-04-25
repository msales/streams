package sql

import (
	"database/sql"

	"github.com/msales/streams"
)

type SqlTxFunc func(*sql.Tx) error

type SqlInsertFunc func(*sql.Tx, interface{}, interface{}) error

type SqlSinkFunc func(*SqlSink)

func WithBeginFn(fn SqlTxFunc) SqlSinkFunc {
	return func(s *SqlSink) {
		s.beginFn = fn
	}
}

func WithCommitFn(fn SqlTxFunc) SqlSinkFunc {
	return func(s *SqlSink) {
		s.commitFn = fn
	}
}

type SqlSink struct {
	ctx streams.Context

	db *sql.DB
	tx *sql.Tx

	beginFn  SqlTxFunc
	insertFn SqlInsertFunc
	commitFn SqlTxFunc

	batch int
	count int
}

// NewSqlSink creates a new batch sql insert sink.
func NewSqlSink(db *sql.DB, fn SqlInsertFunc, batch int, opts ...SqlSinkFunc) *SqlSink {
	s := &SqlSink{
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
func (p *SqlSink) WithContext(ctx streams.Context) {
	p.ctx = ctx
}

// Process processes the stream record.
func (p *SqlSink) Process(key, value interface{}) error {
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

func (p *SqlSink) ensureTransaction() error {
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

func (p *SqlSink) commitTransaction() error {
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
func (p *SqlSink) Close() error {
	if err := p.commitTransaction(); err != nil {
		return err
	}

	return p.db.Close()
}
