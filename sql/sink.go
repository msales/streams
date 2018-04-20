package sql

import (
	"database/sql"

	"github.com/msales/streams"
)

type SqlInsertFunc func(*sql.Tx, interface{}, interface{}) error

type SqlSink struct {
	ctx streams.Context

	db *sql.DB
	tx *sql.Tx
	fn SqlInsertFunc

	batch int
	count int
}

// NewSqlSink creates a new batch sql insert sink.
func NewSqlSink(db *sql.DB, fn SqlInsertFunc, batch int) *SqlSink {
	return &SqlSink{
		db:    db,
		fn:    fn,
		batch: batch,
		count: 0,
	}
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

	if err := p.fn(p.tx, key, value); err != nil {
		return err
	}

	p.count++
	if p.count >= p.batch {
		p.count = 0
		return p.commitTransaction()
	}

	return nil
}

func (p *SqlSink) ensureTransaction() (err error) {
	if p.tx != nil {
		return
	}

	p.tx, err = p.db.Begin()
	return
}

func (p *SqlSink) commitTransaction() (err error) {
	if p.tx == nil {
		return
	}

	if err = p.tx.Commit(); err != nil {
		p.tx.Rollback()
		return
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
