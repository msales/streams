package sql_test

import (
	sql2 "database/sql"
	"testing"

	"github.com/msales/streams"
	"github.com/msales/streams/mocks"
	"github.com/msales/streams/sql"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestNewSink(t *testing.T) {
	db, _, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	s, err := sql.NewSink(db, func(*sql2.Tx, *streams.Message) error {
		return nil
	}, sql.WithBatchMessages(1))

	assert.NoError(t, err)
	assert.IsType(t, &sql.Sink{}, s)
}

func TestNewSinkMustHaveBatch(t *testing.T) {
	db, _, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	_, err = sql.NewSink(db, func(*sql2.Tx, *streams.Message) error {
		return nil
	})

	assert.Error(t, err)
}

func TestSink_Process(t *testing.T) {
	insertCalled := false

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()

	pipe := mocks.NewPipe(t)
	pipe.ExpectMark("test", "test")
	s, _ := sql.NewSink(db, func(*sql2.Tx, *streams.Message) error {
		insertCalled = true
		return nil
	}, sql.WithBatchMessages(10))
	s.WithPipe(pipe)

	err = s.Process(streams.NewMessage("test", "test"))

	assert.NoError(t, err)
	assert.True(t, insertCalled)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("There were unfulfilled expectations: %s", err)
	}
}

func TestSink_ProcessWithTxError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin().WillReturnError(errors.New("test error"))

	pipe := mocks.NewPipe(t)
	s, _ := sql.NewSink(db, func(*sql2.Tx, *streams.Message) error {
		return nil
	}, sql.WithBatchMessages(10))
	s.WithPipe(pipe)

	err = s.Process(streams.NewMessage("test", "test"))

	assert.Error(t, err)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("There were unfulfilled expectations: %s", err)
	}
}

func TestSink_ProcessWithInsertError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()

	pipe := mocks.NewPipe(t)
	s, _ := sql.NewSink(db, func(*sql2.Tx, *streams.Message) error {
		return errors.New("test error")
	}, sql.WithBatchMessages(10))
	s.WithPipe(pipe)

	err = s.Process(streams.NewMessage("test", "test"))

	assert.Error(t, err)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("There were unfulfilled expectations: %s", err)
	}
}

func TestSink_ProcessWithTxFuncs(t *testing.T) {
	beginCalled := false
	insertCalled := false

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()

	pipe := mocks.NewPipe(t)
	pipe.ExpectCommit()

	s, _ := sql.NewSink(db, func(*sql2.Tx, *streams.Message) error {
		insertCalled = true
		return nil
	}, sql.WithBatchMessages(1), sql.WithBeginFn(func(tx *sql2.Tx) error {
		beginCalled = true
		return nil
	}))
	s.WithPipe(pipe)

	err = s.Process(streams.NewMessage("test", "test"))

	assert.NoError(t, err)
	assert.True(t, beginCalled)
	assert.True(t, insertCalled)
	pipe.AssertExpectations()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("There were unfulfilled expectations: %s", err)
	}
}

func TestSink_ProcessWithBeginFuncError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()

	pipe := mocks.NewPipe(t)
	s, _ := sql.NewSink(db, func(*sql2.Tx, *streams.Message) error {
		return nil
	}, sql.WithBatchMessages(10), sql.WithBeginFn(func(tx *sql2.Tx) error {
		return errors.New("test error")
	}))
	s.WithPipe(pipe)

	err = s.Process(streams.NewMessage("test", "test"))

	assert.Error(t, err)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("There were unfulfilled expectations: %s", err)
	}
}

func TestSink_ProcessWithMessageCommit(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()

	pipe := mocks.NewPipe(t)
	pipe.ExpectMark("test", "test")
	pipe.ExpectCommit()

	s, _ := sql.NewSink(db, func(*sql2.Tx, *streams.Message) error {
		return nil
	}, sql.WithBatchMessages(2))
	s.WithPipe(pipe)

	s.Process(streams.NewMessage("test", "test"))
	err = s.Process(streams.NewMessage("test1", "test1"))

	assert.NoError(t, err)
	pipe.AssertExpectations()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("There were unfulfilled expectations: %s", err)
	}
}

func TestSink_Commit(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	pipe := mocks.NewPipe(t)
	pipe.ExpectMark(mocks.Anything, mocks.Anything)
	s, _ := sql.NewSink(db, func(*sql2.Tx, *streams.Message) error {
		return nil
	}, sql.WithBatchMessages(2))
	s.WithPipe(pipe)
	s.Process(streams.NewMessage("test1", "test1"))

	err = s.Commit()

	assert.NoError(t, err)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("There were unfulfilled expectations: %s", err)
	}
}

func TestSink_CommitNoTransaction(t *testing.T) {
	db, _, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	pipe := mocks.NewPipe(t)
	s, _ := sql.NewSink(db, func(*sql2.Tx, *streams.Message) error {
		return nil
	}, sql.WithBatchMessages(2))
	s.WithPipe(pipe)

	err = s.Commit()

	assert.NoError(t, err)
}

func TestSink_CommitFuncError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()

	pipe := mocks.NewPipe(t)
	pipe.ExpectMark(mocks.Anything, mocks.Anything)
	s, _ := sql.NewSink(db, func(*sql2.Tx, *streams.Message) error {
		return nil
	}, sql.WithBatchMessages(2), sql.WithCommitFn(func(tx *sql2.Tx) error {
		return errors.New("test error")
	}))
	s.WithPipe(pipe)
	s.Process(streams.NewMessage("test1", "test1"))

	err = s.Commit()

	assert.Error(t, err)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("There were unfulfilled expectations: %s", err)
	}
}

func TestSink_CommitError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit().WillReturnError(errors.New("test error"))

	pipe := mocks.NewPipe(t)
	pipe.ExpectMark(mocks.Anything, mocks.Anything)
	s, _ := sql.NewSink(db, func(*sql2.Tx, *streams.Message) error {
		return nil
	}, sql.WithBatchMessages(2))
	s.WithPipe(pipe)
	s.Process(streams.NewMessage("test1", "test1"))

	err = s.Commit()

	assert.Error(t, err)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("There were unfulfilled expectations: %s", err)
	}
}

func TestSink_Close(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock.ExpectClose()

	pipe := mocks.NewPipe(t)
	s, _ := sql.NewSink(db, func(*sql2.Tx, *streams.Message) error {
		return nil
	}, sql.WithBatchMessages(10))
	s.WithPipe(pipe)

	err = s.Close()

	assert.NoError(t, err)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("There were unfulfilled expectations: %s", err)
	}
}

func TestSink_CloseWithTxRollback(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectRollback()
	mock.ExpectClose()

	pipe := mocks.NewPipe(t)
	pipe.ExpectMark(mocks.Anything, mocks.Anything)
	s, _ := sql.NewSink(db, func(*sql2.Tx, *streams.Message) error {
		return nil
	}, sql.WithBatchMessages(10))
	s.WithPipe(pipe)
	err = s.Process(streams.NewMessage("test", "test"))
	assert.NoError(t, err)

	err = s.Close()

	assert.NoError(t, err)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("There were unfulfilled expectations: %s", err)
	}
}
