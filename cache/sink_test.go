package cache_test

import (
	"errors"
	"testing"
	"time"

	cache2 "github.com/msales/pkg/v4/cache"
	"github.com/msales/streams/v4"
	"github.com/msales/streams/v4/cache"
	"github.com/msales/streams/v4/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNewSink(t *testing.T) {
	s := cache.NewSink(cache2.Null, time.Millisecond, 1)

	assert.IsType(t, &cache.Sink{}, s)
}

func TestSink_Process(t *testing.T) {
	c := new(MockCache)
	c.On("Set", "test", "test", time.Millisecond).Return(nil)
	pipe := mocks.NewPipe(t)
	pipe.ExpectMark("test", "test")
	s := cache.NewSink(c, time.Millisecond, 10)
	s.WithPipe(pipe)

	err := s.Process(streams.NewMessage("test", "test"))

	assert.NoError(t, err)
	c.AssertExpectations(t)
}

func TestSink_ProcessWithCommit(t *testing.T) {
	c := new(MockCache)
	c.On("Set", "test", "test", time.Millisecond).Return(nil)
	pipe := mocks.NewPipe(t)
	pipe.ExpectCommit()
	s := cache.NewSink(c, time.Millisecond, 1)
	s.WithPipe(pipe)

	err := s.Process(streams.NewMessage("test", "test"))

	assert.NoError(t, err)
	c.AssertExpectations(t)
	pipe.AssertExpectations()
}

func TestSink_ProcessWithCacheError(t *testing.T) {
	c := new(MockCache)
	c.On("Set", "test", "test", time.Millisecond).Return(errors.New("test error"))
	pipe := mocks.NewPipe(t)
	s := cache.NewSink(c, time.Millisecond, 1)
	s.WithPipe(pipe)

	err := s.Process(streams.NewMessage("test", "test"))

	assert.Error(t, err)
	c.AssertExpectations(t)
}

func TestSink_Close(t *testing.T) {
	pipe := mocks.NewPipe(t)
	s := cache.NewSink(cache2.Null, time.Millisecond, 1)
	s.WithPipe(pipe)

	err := s.Close()

	assert.NoError(t, err)
}

type MockCache struct {
	mock.Mock
}

func (c *MockCache) Get(key string) *cache2.Item {
	args := c.Called(key)
	return args.Get(0).(*cache2.Item)
}

func (c *MockCache) GetMulti(keys ...string) ([]*cache2.Item, error) {
	args := c.Called(keys)
	return args.Get(0).([]*cache2.Item), args.Error(1)
}

func (c *MockCache) Set(key string, value interface{}, expire time.Duration) error {
	args := c.Called(key, value, expire)
	return args.Error(0)
}

func (c *MockCache) Add(key string, value interface{}, expire time.Duration) error {
	args := c.Called(key, value, expire)
	return args.Error(0)
}

func (c *MockCache) Replace(key string, value interface{}, expire time.Duration) error {
	args := c.Called(key, value, expire)
	return args.Error(0)
}

func (c *MockCache) Delete(key string) error {
	args := c.Called(key)
	return args.Error(0)
}

func (c *MockCache) Inc(key string, value uint64) (int64, error) {
	args := c.Called(key, value)
	return args.Get(0).(int64), args.Error(1)
}

func (c *MockCache) Dec(key string, value uint64) (int64, error) {
	args := c.Called(key, value)
	return args.Get(0).(int64), args.Error(1)
}
