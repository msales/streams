package streams_test

import (
	"testing"

	"github.com/msales/streams"
	"github.com/stretchr/testify/assert"
)

func TestMetastore_PullAll(t *testing.T) {
	p := new(MockProcessor)
	src := new(MockSource)
	meta := new(MockMetadata)
	s := streams.NewMetastore()
	_ = s.Mark(p, src, meta)

	procMeta, err := s.PullAll()

	assert.NoError(t, err)
	assert.Equal(t, map[streams.Processor][]streams.Metaitem{p: {{Source: src, Metadata: meta}}}, procMeta)
}

func TestMetastore_PullAllClearsMetastore(t *testing.T) {
	p := new(MockProcessor)
	src := new(MockSource)
	meta := new(MockMetadata)
	s := streams.NewMetastore()
	_ = s.Mark(p, src, meta)

	_, _ = s.PullAll()
	procMeta, err := s.PullAll()

	assert.NoError(t, err)
	assert.Equal(t, map[streams.Processor][]streams.Metaitem{}, procMeta)
}

func TestMetastore_Mark(t *testing.T) {
	p := new(MockProcessor)
	src := new(MockSource)
	meta1 := new(MockMetadata)
	meta2 := new(MockMetadata)
	meta3 := new(MockMetadata)
	meta2.On("Merge", meta1).Return(meta3)
	s := streams.NewMetastore()

	err := s.Mark(p, src, meta1)
	assert.NoError(t, err)

	err = s.Mark(p, src, meta2)
	assert.NoError(t, err)

	procMeta, _ := s.PullAll()
	assert.Equal(t, map[streams.Processor][]streams.Metaitem{p: {{Source: src, Metadata: meta3}}}, procMeta)
}

func TestMetastore_MarkMultipleSources(t *testing.T) {
	p := new(MockProcessor)
	src1 := new(MockSource)
	src2 := new(MockSource)
	meta := new(MockMetadata)
	s := streams.NewMetastore()

	err := s.Mark(p, src1, meta)
	assert.NoError(t, err)

	err = s.Mark(p, src2, meta)
	assert.NoError(t, err)

	procMeta, _ := s.PullAll()
	assert.Equal(t, map[streams.Processor][]streams.Metaitem{p: {{Source: src1, Metadata: meta}, {Source: src2, Metadata: meta}}}, procMeta)
}

func TestMetastore_Pull(t *testing.T) {
	p := new(MockProcessor)
	src := new(MockSource)
	meta := new(MockMetadata)
	s := streams.NewMetastore()
	_ = s.Mark(p, src, meta)

	pulled, err := s.Pull(p)

	assert.NoError(t, err)
	assert.Equal(t, []streams.Metaitem{{Source: src, Metadata: meta}}, pulled)
}

func TestMetastore_PullClearsProcessor(t *testing.T) {
	p := new(MockProcessor)
	src := new(MockSource)
	meta := new(MockMetadata)
	s := streams.NewMetastore()
	_ = s.Mark(p, src, meta)

	_, _ = s.Pull(p)
	pulled, err := s.Pull(p)

	assert.NoError(t, err)
	assert.Equal(t, []streams.Metaitem(nil), pulled)
}

func TestMetastore_PullReturnsNilIfDoesntExist(t *testing.T) {
	p := new(MockProcessor)
	s := streams.NewMetastore()

	pulled, err := s.Pull(p)

	assert.NoError(t, err)
	assert.Equal(t, []streams.Metaitem(nil), pulled)
}

func BenchmarkMetastore_Mark(b *testing.B) {
	p := new(MockProcessor)
	src := new(MockSource)
	meta := &fakeMetadata{}
	s := streams.NewMetastore()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Mark(p, src, meta)
	}
}

type fakeMetadata struct{}

func (m *fakeMetadata) Merge(v streams.Metadata) streams.Metadata {
	return m
}
