package sql_test

import (
	sql2 "database/sql"
	"testing"

	"github.com/msales/streams"
	"github.com/msales/streams/mocks"
	"github.com/msales/streams/sql"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

func TestNewSink(t *testing.T) {
	db, _, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	s := sql.NewSink(db, func(*sql2.Tx, *streams.Message) error {
		return nil
	}, 1)

	assert.IsType(t, &sql.Sink{}, s)
}

func TestSink_Process(t *testing.T) {
	insertCalled := false

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()

	pipe := mocks.NewPipe(t)
	s := sql.NewSink(db, func(*sql2.Tx, *streams.Message) error {
		insertCalled = true
		return nil
	}, 10)
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
	s := sql.NewSink(db, func(*sql2.Tx, *streams.Message) error {
		return nil
	}, 10)
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
	s := sql.NewSink(db, func(*sql2.Tx, *streams.Message) error {
		return errors.New("test error")
	}, 10)
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
	commitCalled := false

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	pipe := mocks.NewPipe(t)
	pipe.ExpectCommit()

	s := sql.NewSink(db, func(*sql2.Tx, *streams.Message) error {
		insertCalled = true
		return nil
	}, 1, sql.WithBeginFn(func(tx *sql2.Tx) error {
		beginCalled = true
		return nil
	}), sql.WithCommitFn(func(tx *sql2.Tx) error {
		commitCalled = true
		return nil
	}))
	s.WithPipe(pipe)

	err = s.Process(streams.NewMessage("test", "test"))

	assert.NoError(t, err)
	assert.True(t, beginCalled)
	assert.True(t, insertCalled)
	assert.True(t, commitCalled)
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
	s := sql.NewSink(db, func(*sql2.Tx, *streams.Message) error {
		return nil
	}, 10, sql.WithBeginFn(func(tx *sql2.Tx) error {
		return errors.New("test error")
	}))
	s.WithPipe(pipe)

	err = s.Process(streams.NewMessage("test", "test"))

	assert.Error(t, err)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("There were unfulfilled expectations: %s", err)
	}
}

func TestSink_ProcessWithCommitFuncError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()

	pipe := mocks.NewPipe(t)
	s := sql.NewSink(db, func(*sql2.Tx, *streams.Message) error {
		return nil
	}, 1, sql.WithCommitFn(func(tx *sql2.Tx) error {
		return errors.New("test error")
	}))
	s.WithPipe(pipe)

	err = s.Process(streams.NewMessage("test", "test"))

	assert.Error(t, err)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("There were unfulfilled expectations: %s", err)
	}
}

func TestSink_ProcessWithCommit(t *testing.T) {
	insertCalled := false

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	pipe := mocks.NewPipe(t)
	pipe.ExpectCommit()

	s := sql.NewSink(db, func(*sql2.Tx, *streams.Message) error {
		insertCalled = true
		return nil
	}, 2)
	s.WithPipe(pipe)

	s.Process(streams.NewMessage("test", "test"))
	err = s.Process(streams.NewMessage("test", "test"))

	assert.NoError(t, err)
	pipe.AssertExpectations()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("There were unfulfilled expectations: %s", err)
	}
}

func TestSink_ProcessWithCommitError(t *testing.T) {
	insertCalled := false

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit().WillReturnError(errors.New("test error"))

	pipe := mocks.NewPipe(t)
	s := sql.NewSink(db, func(*sql2.Tx, *streams.Message) error {
		insertCalled = true
		return nil
	}, 1)
	s.WithPipe(pipe)

	err = s.Process(streams.NewMessage("test", "test"))

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
	s := sql.NewSink(db, func(*sql2.Tx, *streams.Message) error {
		return nil
	}, 10)
	s.WithPipe(pipe)

	err = s.Close()

	assert.NoError(t, err)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("There were unfulfilled expectations: %s", err)
	}
}

func TestSink_CloseWithTxError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit().WillReturnError(errors.New("test error"))

	pipe := mocks.NewPipe(t)
	s := sql.NewSink(db, func(*sql2.Tx, *streams.Message) error {
		return nil
	}, 10)
	s.WithPipe(pipe)
	err = s.Process(streams.NewMessage("test", "test"))
	assert.NoError(t, err)

	err = s.Close()

	assert.Error(t, err)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("There were unfulfilled expectations: %s", err)
	}
}
