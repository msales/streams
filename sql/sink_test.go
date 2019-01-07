package sql_test

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/msales/streams/v2"
	"github.com/msales/streams/v2/mocks"
	sqlx "github.com/msales/streams/v2/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

func newDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	db, m, err := sqlmock.New()
	assert.NoError(t, err)

	return db, m
}

func TestExecFunc_ImplementsExecutor(t *testing.T) {
	exec := sqlx.ExecFunc(func(*sql.Tx, streams.Message) error {
		return nil
	})

	assert.Implements(t, (*sqlx.Executor)(nil), exec)
}

func TestExecFunc_Exec(t *testing.T) {
	called := false
	exec := sqlx.ExecFunc(func(*sql.Tx, streams.Message) error {
		called = true
		return nil
	})

	err := exec.Exec(nil, streams.EmptyMessage)

	assert.NoError(t, err)
	assert.True(t, called)
}

func TestExecFunc_ExecError(t *testing.T) {
	exec := sqlx.ExecFunc(func(*sql.Tx, streams.Message) error {
		return errors.New("test")
	})

	err := exec.Exec(nil, streams.EmptyMessage)

	assert.Error(t, err)
}

func TestNewSink(t *testing.T) {
	db, _ := newDB(t)
	defer db.Close()

	exec := new(MockExecutor)

	s, err := sqlx.NewSink(db, 1, exec)

	assert.NoError(t, err)
	assert.IsType(t, &sqlx.Sink{}, s)
}

func TestNewSinkMustHaveBatch(t *testing.T) {
	db, _ := newDB(t)
	defer db.Close()

	exec := new(MockExecutor)

	_, err := sqlx.NewSink(db, 0, exec)

	assert.Error(t, err)
}

func TestSink_Process(t *testing.T) {
	db, dbMock := newDB(t)
	defer db.Close()
	dbMock.ExpectBegin()

	exec := new(MockExecutor)
	exec.On("Exec", mock.Anything, mock.Anything).Return(nil)

	pipe := mocks.NewPipe(t)
	pipe.ExpectMark("test", "test")

	s, _ := sqlx.NewSink(db, 10, exec)
	s.WithPipe(pipe)

	msg := streams.NewMessage("test", "test")

	err := s.Process(msg)

	assert.NoError(t, err)
	exec.AssertCalled(t, "Exec", mock.Anything, msg)
	if err := dbMock.ExpectationsWereMet(); err != nil {
		t.Errorf("There were unfulfilled expectations: %s", err)
	}
}

func TestSink_ProcessWithTxError(t *testing.T) {
	db, dbMock := newDB(t)
	defer db.Close()
	dbMock.ExpectBegin().WillReturnError(errors.New("test error"))

	exec := new(MockExecutor)
	pipe := mocks.NewPipe(t)
	s, _ := sqlx.NewSink(db, 10, exec)
	s.WithPipe(pipe)

	err := s.Process(streams.NewMessage("test", "test"))

	assert.Error(t, err)
	if err := dbMock.ExpectationsWereMet(); err != nil {
		t.Errorf("There were unfulfilled expectations: %s", err)
	}
}

func TestSink_ProcessWithInsertError(t *testing.T) {
	db, dbMock := newDB(t)
	defer db.Close()
	dbMock.ExpectBegin()

	exec := new(MockExecutor)
	exec.On("Exec", mock.Anything, mock.Anything).Return(errors.New("test"))

	pipe := mocks.NewPipe(t)
	s, _ := sqlx.NewSink(db, 10, exec)
	s.WithPipe(pipe)

	err := s.Process(streams.NewMessage("test", "test"))

	assert.Error(t, err)
}

func TestSink_ProcessWithTxBegin(t *testing.T) {
	db, dbMock := newDB(t)
	defer db.Close()
	dbMock.ExpectBegin()

	exec := new(MockExecutorTx)
	exec.On("Begin", mock.Anything).Return(nil)
	exec.On("Exec", mock.Anything, mock.Anything).Return(nil)

	pipe := mocks.NewPipe(t)
	pipe.ExpectMark("test", "test")

	s, _ := sqlx.NewSink(db, 10, exec)
	s.WithPipe(pipe)

	err := s.Process(streams.NewMessage("test", "test"))

	assert.NoError(t, err)
	exec.AssertExpectations(t)
	if err := dbMock.ExpectationsWereMet(); err != nil {
		t.Errorf("There were unfulfilled expectations: %s", err)
	}
}

func TestSink_ProcessWithTxBeginError(t *testing.T) {
	db, dbMock := newDB(t)
	defer db.Close()
	dbMock.ExpectBegin()

	exec := new(MockExecutorTx)
	exec.On("Begin", mock.Anything).Return(errors.New("test"))

	pipe := mocks.NewPipe(t)
	s, _ := sqlx.NewSink(db, 1, exec)
	s.WithPipe(pipe)

	err := s.Process(streams.NewMessage("test", "test"))

	assert.Error(t, err)
}

func TestSink_ProcessWithMessageCommit(t *testing.T) {
	db, dbMock := newDB(t)
	defer db.Close()
	dbMock.ExpectBegin()

	exec := new(MockExecutor)
	exec.On("Exec", mock.Anything, mock.Anything).Return(nil)

	pipe := mocks.NewPipe(t)
	pipe.ExpectMark("test", "test")
	pipe.ExpectCommit()

	s, _ := sqlx.NewSink(db, 2, exec)
	s.WithPipe(pipe)

	_ = s.Process(streams.NewMessage("test", "test"))
	err := s.Process(streams.NewMessage("test1", "test1"))

	assert.NoError(t, err)
	pipe.AssertExpectations()
}

func TestSink_Commit(t *testing.T) {
	db, dbMock := newDB(t)
	defer db.Close()
	dbMock.ExpectBegin()
	dbMock.ExpectCommit()

	exec := new(MockExecutor)
	exec.On("Exec", mock.Anything, mock.Anything).Return(nil)

	pipe := mocks.NewPipe(t)
	pipe.ExpectMark(mocks.Anything, mocks.Anything)

	s, _ := sqlx.NewSink(db, 2, exec)
	s.WithPipe(pipe)
	_ = s.Process(streams.NewMessage("test1", "test1"))

	err := s.Commit()

	assert.NoError(t, err)
	if err := dbMock.ExpectationsWereMet(); err != nil {
		t.Errorf("There were unfulfilled expectations: %s", err)
	}
}

func TestSink_CommitNoTransaction(t *testing.T) {
	db, _ := newDB(t)
	defer db.Close()

	exec := new(MockExecutor)
	pipe := mocks.NewPipe(t)
	s, _ := sqlx.NewSink(db, 2, exec)
	s.WithPipe(pipe)

	err := s.Commit()

	assert.NoError(t, err)
}

func TestSink_CommitTxCommit(t *testing.T) {
	db, dbMock := newDB(t)
	defer db.Close()
	dbMock.ExpectBegin()
	dbMock.ExpectCommit()

	exec := new(MockExecutorTx)
	exec.On("Begin", mock.Anything).Return(nil)
	exec.On("Exec", mock.Anything, mock.Anything).Return(nil)
	exec.On("Commit", mock.Anything).Return(nil)

	pipe := mocks.NewPipe(t)
	pipe.ExpectMark(mocks.Anything, mocks.Anything)

	s, _ := sqlx.NewSink(db, 2, exec)
	s.WithPipe(pipe)
	_ = s.Process(streams.NewMessage("test1", "test1"))

	err := s.Commit()

	assert.NoError(t, err)
	exec.AssertExpectations(t)
}

func TestSink_CommitTxCommitError(t *testing.T) {
	db, dbMock := newDB(t)
	defer db.Close()
	dbMock.ExpectBegin()

	exec := new(MockExecutorTx)
	exec.On("Begin", mock.Anything).Return(nil)
	exec.On("Exec", mock.Anything, mock.Anything).Return(nil)
	exec.On("Commit", mock.Anything).Return(errors.New("test"))

	pipe := mocks.NewPipe(t)
	pipe.ExpectMark(mocks.Anything, mocks.Anything)

	s, _ := sqlx.NewSink(db, 2, exec)
	s.WithPipe(pipe)
	_ = s.Process(streams.NewMessage("test1", "test1"))

	err := s.Commit()

	assert.Error(t, err)
}

func TestSink_CommitDBError(t *testing.T) {
	db, dbMock := newDB(t)
	defer db.Close()
	dbMock.ExpectBegin()
	dbMock.ExpectCommit().WillReturnError(errors.New("test error"))

	exec := new(MockExecutorTx)
	exec.On("Begin", mock.Anything).Return(nil)
	exec.On("Exec", mock.Anything, mock.Anything).Return(nil)
	exec.On("Commit", mock.Anything).Return(nil)

	pipe := mocks.NewPipe(t)
	pipe.ExpectMark(mocks.Anything, mocks.Anything)

	s, _ := sqlx.NewSink(db, 2, exec)
	s.WithPipe(pipe)
	_ = s.Process(streams.NewMessage("test1", "test1"))

	err := s.Commit()

	assert.Error(t, err)
	if err := dbMock.ExpectationsWereMet(); err != nil {
		t.Errorf("There were unfulfilled expectations: %s", err)
	}
}

func TestSink_Close(t *testing.T) {
	db, dbMock := newDB(t)
	defer db.Close()
	dbMock.ExpectClose()

	exec := new(MockExecutor)
	pipe := mocks.NewPipe(t)
	s, _ := sqlx.NewSink(db, 10, exec)
	s.WithPipe(pipe)

	err := s.Close()

	assert.NoError(t, err)
	if err := dbMock.ExpectationsWereMet(); err != nil {
		t.Errorf("There were unfulfilled expectations: %s", err)
	}
}

func TestSink_CloseWithTxRollback(t *testing.T) {
	db, dbMock := newDB(t)
	defer db.Close()
	dbMock.ExpectBegin()
	dbMock.ExpectRollback()
	dbMock.ExpectClose()

	exec := new(MockExecutor)
	exec.On("Exec", mock.Anything, mock.Anything).Return(nil)

	pipe := mocks.NewPipe(t)
	pipe.ExpectMark(mocks.Anything, mocks.Anything)

	s, _ := sqlx.NewSink(db, 10, exec)
	s.WithPipe(pipe)
	_ = s.Process(streams.NewMessage("test", "test"))

	err := s.Close()

	assert.NoError(t, err)
	if err := dbMock.ExpectationsWereMet(); err != nil {
		t.Errorf("There were unfulfilled expectations: %s", err)
	}
}

type MockExecutor struct {
	mock.Mock
}

func (m *MockExecutor) Exec(tx *sql.Tx, msg streams.Message) error {
	args := m.Called(tx, msg)
	return args.Error(0)
}

type MockExecutorTx struct {
	mock.Mock
}

func (m *MockExecutorTx) Begin(tx *sql.Tx) error {
	args := m.Called(tx)
	return args.Error(0)
}

func (m *MockExecutorTx) Exec(tx *sql.Tx, msg streams.Message) error {
	args := m.Called(tx, msg)
	return args.Error(0)
}

func (m *MockExecutorTx) Commit(tx *sql.Tx) error {
	args := m.Called(tx)
	return args.Error(0)
}
