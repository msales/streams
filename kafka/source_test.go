package kafka_test

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/msales/streams/kafka"
	"github.com/stretchr/testify/assert"
)

func TestNewSourceConfig(t *testing.T) {
	c := kafka.NewSourceConfig()

	assert.IsType(t, &kafka.SourceConfig{}, c)
}

func TestSourceConfig_Validate(t *testing.T) {
	c := kafka.NewSourceConfig()
	c.Brokers = []string{"test"}

	err := c.Validate()

	assert.NoError(t, err)
}

func TestSourceConfig_ValidateErrors(t *testing.T) {
	tests := []struct {
		name string
		cfg  func(*kafka.SourceConfig)
		err  string
	}{
		{
			name: "Brokers",
			cfg: func(c *kafka.SourceConfig) {
				c.Brokers = []string{}
			},
			err: "Brokers must have at least one broker",
		},
		{
			name: "KeyDecoder",
			cfg: func(c *kafka.SourceConfig) {
				c.Brokers = []string{"test"}
				c.KeyDecoder = nil
			},
			err: "KeyDecoder must be an instance of Decoder",
		},
		{
			name: "ValueDecoder",
			cfg: func(c *kafka.SourceConfig) {
				c.Brokers = []string{"test"}
				c.ValueDecoder = nil
			},
			err: "ValueDecoder must be an instance of Decoder",
		},
		{
			name: "BufferSize",
			cfg: func(c *kafka.SourceConfig) {
				c.Brokers = []string{"test"}
				c.BufferSize = 0
			},
			err: "BufferSize must be at least 1",
		},
		{
			name: "BaseConfig",
			cfg: func(c *kafka.SourceConfig) {
				c.Brokers = []string{"test"}
				c.Metadata.Retry.Max = -1
			},
			err: "Metadata.Retry.Max must be >= 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := kafka.NewSourceConfig()
			tt.cfg(c)

			err := c.Validate()

			assert.Equal(t, tt.err, string(err.(sarama.ConfigurationError)))
		})
	}
}

func TestMergedMetadata_Merge(t *testing.T) {
	a := kafka.MergedMetadata{0: 3}
	b := kafka.MergedMetadata{1: 2}

	res := b.Merge(a)

	assert.IsType(t, kafka.MergedMetadata{}, res)
	a = res.(kafka.MergedMetadata)
	assert.Equal(t, kafka.MergedMetadata{0: 3, 1: 2}, a)
}

func TestMergedMetadata_MergePicksLowest(t *testing.T) {
	a := kafka.MergedMetadata{0: 10}
	b := kafka.MergedMetadata{0: 2}

	res := b.Merge(a)

	assert.IsType(t, kafka.MergedMetadata{}, res)
	a = res.(kafka.MergedMetadata)
	assert.Equal(t, kafka.MergedMetadata{0: 2}, a)
}

func TestMergedMetadata_MergeNilMerged(t *testing.T) {
	b := kafka.MergedMetadata{0: 5}

	res := b.Merge(nil)

	assert.IsType(t, kafka.MergedMetadata{}, res)
	a := res.(kafka.MergedMetadata)
	assert.Equal(t, kafka.MergedMetadata{0: 5}, a)
}

func TestMetadata_Merge(t *testing.T) {
	merged := kafka.MergedMetadata{0: 3, 1: 10}
	meta := &kafka.Metadata{Topic: "foo", Partition: 1, Offset: 2}

	res := meta.Merge(merged)

	assert.IsType(t, kafka.MergedMetadata{}, res)
	merged = res.(kafka.MergedMetadata)
	assert.Equal(t, kafka.MergedMetadata{0: 3, 1: 2}, merged)
}

func TestMetadata_MergePicksLowest(t *testing.T) {
	merged := kafka.MergedMetadata{0: 3}
	meta := &kafka.Metadata{Topic: "foo", Partition: 0, Offset: 10}

	res := meta.Merge(merged)

	assert.IsType(t, kafka.MergedMetadata{}, res)
	merged = res.(kafka.MergedMetadata)
	assert.Equal(t, kafka.MergedMetadata{0: 3}, merged)
}

func TestMetadata_MergeNilMerged(t *testing.T) {
	meta := &kafka.Metadata{Topic: "baz", Partition: 1, Offset: 10}

	res := meta.Merge(nil)

	assert.IsType(t, kafka.MergedMetadata{}, res)
	merged := res.(kafka.MergedMetadata)
	assert.Equal(t, kafka.MergedMetadata{1: 10}, merged)
}

func BenchmarkMergedMetadata_Merge(b *testing.B) {
	meta := kafka.MergedMetadata{1: 2}
	other := kafka.MergedMetadata{2: 2}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		meta.Merge(other)
	}
}

func TestNewSource(t *testing.T) {
	broker0 := sarama.NewMockBroker(t, 0)
	defer broker0.Close()
	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader("test_topic", 0, broker0.BrokerID()),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t).
			SetCoordinator(sarama.CoordinatorGroup, "test_group", broker0),
		"JoinGroupRequest": sarama.NewMockWrapper(&sarama.JoinGroupResponse{
			Version:       1,
			Err:           sarama.ErrNoError,
			GroupProtocol: "protocol",
		}),
		"SyncGroupRequest": sarama.NewMockWrapper(&sarama.SyncGroupResponse{
			Err: sarama.ErrNoError,
			MemberAssignment: []byte{
				0, 1,                                                    // Version
				0, 0, 0, 1,                                              // Topic array length
				0, 10, 't', 'e', 's', 't', '_', 't', 'o', 'p', 'i', 'c', // Topic one
				0, 0, 0, 1,                                              // Topic one, partition array length
				0, 0, 0, 0,                                              // 0
				0, 0, 0, 3, 0x01, 0x02, 0x03,                            // Userdata
			},
		}),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(t),
		"LeaveGroupRequest": sarama.NewMockWrapper(&sarama.LeaveGroupResponse{
			Err: sarama.ErrNoError,
		}),
	})
	c := kafka.NewSourceConfig()
	c.Brokers = []string{broker0.Addr()}
	c.Topic = "test_topic"
	c.GroupID = "test_group"

	s, err := kafka.NewSource(c)

	time.Sleep(100 * time.Millisecond)

	assert.NoError(t, err)
	assert.IsType(t, &kafka.Source{}, s)

	if s != nil {
		s.Close()
	}
}

func TestNewSource_ValidatesConfig(t *testing.T) {
	c := kafka.NewSourceConfig()

	_, err := kafka.NewSource(c)

	assert.Error(t, err)
}

func TestNewSource_Error(t *testing.T) {
	broker0 := sarama.NewMockBroker(t, 0)
	broker0.Close()
	c := kafka.NewSourceConfig()
	c.Brokers = []string{broker0.Addr()}
	c.Topic = "test_topic"
	c.GroupID = "test_group"

	_, err := kafka.NewSource(c)

	assert.Error(t, err)
}

func TestSource_Consume(t *testing.T) {
	broker0 := sarama.NewMockBroker(t, 0)
	defer broker0.Close()
	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader("test_topic", 0, broker0.BrokerID()),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t).
			SetCoordinator(sarama.CoordinatorGroup, "test_group", broker0),
		"JoinGroupRequest": sarama.NewMockWrapper(&sarama.JoinGroupResponse{
			Version:       1,
			Err:           sarama.ErrNoError,
			GroupProtocol: "protocol",
		}),
		"SyncGroupRequest": sarama.NewMockWrapper(&sarama.SyncGroupResponse{
			Err: sarama.ErrNoError,
			MemberAssignment: []byte{
				0, 1,                                                    // Version
				0, 0, 0, 1,                                              // Topic array length
				0, 10, 't', 'e', 's', 't', '_', 't', 'o', 'p', 'i', 'c', // Topic one
				0, 0, 0, 1,                                              // Topic one, partition array length
				0, 0, 0, 0,                                              // 0
				0, 0, 0, 3, 0x01, 0x02, 0x03,                            // Userdata
			},
		}),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(t).
			SetOffset("test_group", "test_topic", 0, 10, "", sarama.ErrNoError),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset("test_topic", 0, sarama.OffsetNewest, 10).
			SetOffset("test_topic", 0, sarama.OffsetOldest, 7),
		"FetchRequest": sarama.NewMockFetchResponse(t, 1).
			SetMessage("test_topic", 0, 10, sarama.StringEncoder("foo")).
			SetMessage("test_topic", 0, 11, sarama.StringEncoder("bar")).
			SetMessage("test_topic", 0, 12, sarama.StringEncoder("baz")).
			SetHighWaterMark("test_topic", 0, 14),
		"LeaveGroupRequest": sarama.NewMockWrapper(&sarama.LeaveGroupResponse{
			Err: sarama.ErrNoError,
		}),
	})
	c := kafka.NewSourceConfig()
	c.Brokers = []string{broker0.Addr()}
	c.Topic = "test_topic"
	c.GroupID = "test_group"
	s, _ := kafka.NewSource(c)
	defer s.Close()

	time.Sleep(100 * time.Millisecond)

	msg, err := s.Consume()

	assert.NoError(t, err)
	assert.Equal(t, []byte(nil), msg.Key)
	assert.Equal(t, []byte("foo"), msg.Value)
}

func TestSource_Commit(t *testing.T) {
	broker0 := sarama.NewMockBroker(t, 0)
	defer broker0.Close()
	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader("test_topic", 0, broker0.BrokerID()),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t).
			SetCoordinator(sarama.CoordinatorGroup, "test_group", broker0),
		"JoinGroupRequest": sarama.NewMockWrapper(&sarama.JoinGroupResponse{
			Version:       1,
			Err:           sarama.ErrNoError,
			GroupProtocol: "protocol",
		}),
		"SyncGroupRequest": sarama.NewMockWrapper(&sarama.SyncGroupResponse{
			Err: sarama.ErrNoError,
			MemberAssignment: []byte{
				0, 1,                                                    // Version
				0, 0, 0, 1,                                              // Topic array length
				0, 10, 't', 'e', 's', 't', '_', 't', 'o', 'p', 'i', 'c', // Topic one
				0, 0, 0, 1,                                              // Topic one, partition array length
				0, 0, 0, 0,                                              // 0
				0, 0, 0, 3, 0x01, 0x02, 0x03,                            // Userdata
			},
		}),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(t).
			SetOffset("test_group", "test_topic", 0, 10, "", sarama.ErrNoError),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset("test_topic", 0, sarama.OffsetNewest, 10).
			SetOffset("test_topic", 0, sarama.OffsetOldest, 7),
		"FetchRequest": sarama.NewMockFetchResponse(t, 1).
			SetMessage("test_topic", 0, 10, sarama.StringEncoder("foo")).
			SetHighWaterMark("test_topic", 0, 14),
		"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(t),
		"LeaveGroupRequest": sarama.NewMockWrapper(&sarama.LeaveGroupResponse{
			Err: sarama.ErrNoError,
		}),
	})
	c := kafka.NewSourceConfig()
	c.Brokers = []string{broker0.Addr()}
	c.Topic = "test_topic"
	c.GroupID = "test_group"
	s, _ := kafka.NewSource(c)
	defer s.Close()
	meta := kafka.MergedMetadata{0: 10}

	time.Sleep(100 * time.Millisecond)

	s.Consume()

	err := s.Commit(meta)

	assert.NoError(t, err)
}

func TestSource_CommitNilMetadata(t *testing.T) {
	broker0 := sarama.NewMockBroker(t, 0)
	defer broker0.Close()
	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader("test_topic", 0, broker0.BrokerID()),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t).
			SetCoordinator(sarama.CoordinatorGroup, "test_group", broker0),
		"JoinGroupRequest": sarama.NewMockWrapper(&sarama.JoinGroupResponse{
			Version:       1,
			Err:           sarama.ErrNoError,
			GroupProtocol: "protocol",
		}),
		"SyncGroupRequest": sarama.NewMockWrapper(&sarama.SyncGroupResponse{
			Err: sarama.ErrNoError,
			MemberAssignment: []byte{
				0, 1,                                                    // Version
				0, 0, 0, 1,                                              // Topic array length
				0, 10, 't', 'e', 's', 't', '_', 't', 'o', 'p', 'i', 'c', // Topic one
				0, 0, 0, 1,                                              // Topic one, partition array length
				0, 0, 0, 0,                                              // 0
				0, 0, 0, 3, 0x01, 0x02, 0x03,                            // Userdata
			},
		}),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(t).
			SetOffset("test_group", "test_topic", 0, 10, "", sarama.ErrNoError),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset("test_topic", 0, sarama.OffsetNewest, 10).
			SetOffset("test_topic", 0, sarama.OffsetOldest, 7),
		"FetchRequest": sarama.NewMockFetchResponse(t, 1).
			SetMessage("test_topic", 0, 10, sarama.StringEncoder("foo")).
			SetHighWaterMark("test_topic", 0, 14),
		"LeaveGroupRequest": sarama.NewMockWrapper(&sarama.LeaveGroupResponse{
			Err: sarama.ErrNoError,
		}),
	})
	c := kafka.NewSourceConfig()
	c.Brokers = []string{broker0.Addr()}
	c.Topic = "test_topic"
	c.GroupID = "test_group"
	s, _ := kafka.NewSource(c)
	defer s.Close()

	time.Sleep(100 * time.Millisecond)

	s.Consume()

	err := s.Commit(nil)

	assert.NoError(t, err)
}

func TestSource_CommitReturnError(t *testing.T) {
	broker0 := sarama.NewMockBroker(t, 0)
	defer broker0.Close()
	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader("test_topic", 0, broker0.BrokerID()),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t).
			SetCoordinator(sarama.CoordinatorGroup, "test_group", broker0),
		"JoinGroupRequest": sarama.NewMockWrapper(&sarama.JoinGroupResponse{
			Version:       1,
			Err:           sarama.ErrNoError,
			GroupProtocol: "protocol",
		}),
		"SyncGroupRequest": sarama.NewMockWrapper(&sarama.SyncGroupResponse{
			Err: sarama.ErrNoError,
			MemberAssignment: []byte{
				0, 1,                                                    // Version
				0, 0, 0, 1,                                              // Topic array length
				0, 10, 't', 'e', 's', 't', '_', 't', 'o', 'p', 'i', 'c', // Topic one
				0, 0, 0, 1,                                              // Topic one, partition array length
				0, 0, 0, 0,                                              // 0
				0, 0, 0, 3, 0x01, 0x02, 0x03,                            // Userdata
			},
		}),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(t).
			SetOffset("test_group", "test_topic", 0, 10, "", sarama.ErrNoError),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset("test_topic", 0, sarama.OffsetNewest, 10).
			SetOffset("test_topic", 0, sarama.OffsetOldest, 7),
		"FetchRequest": sarama.NewMockFetchResponse(t, 1).
			SetMessage("test_topic", 0, 10, sarama.StringEncoder("foo")).
			SetHighWaterMark("test_topic", 0, 14),
		"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(t).
			SetError("test_group", "test_topic", 0, sarama.ErrBrokerNotAvailable),
		"LeaveGroupRequest": sarama.NewMockWrapper(&sarama.LeaveGroupResponse{
			Err: sarama.ErrNoError,
		}),
	})
	c := kafka.NewSourceConfig()
	c.Brokers = []string{broker0.Addr()}
	c.Topic = "test_topic"
	c.GroupID = "test_group"
	s, _ := kafka.NewSource(c)
	defer s.Close()
	meta := kafka.MergedMetadata{0: 10}

	time.Sleep(100 * time.Millisecond)

	s.Consume()

	err := s.Commit(meta)

	assert.Error(t, err)
}
