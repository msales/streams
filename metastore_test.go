package streams_test

import (
	"testing"

	"github.com/msales/streams"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestMetaitems_Join(t *testing.T) {
	src1 := new(MockSource)
	src2 := new(MockSource)
	src3 := new(MockSource)

	meta1 := new(MockMetadata)
	meta2 := new(MockMetadata)
	meta3 := new(MockMetadata)
	meta4 := new(MockMetadata)
	meta5 := new(MockMetadata) // == meta2.Update(meta4)

	item1 := &streams.Metaitem{Source: src1, Metadata: meta1}
	item2 := &streams.Metaitem{Source: src2, Metadata: meta2}
	item3 := &streams.Metaitem{Source: src3, Metadata: meta3}
	item4 := &streams.Metaitem{Source: src2, Metadata: meta4} // src2!

	items := streams.Metaitems{item1, item2}
	other := streams.Metaitems{item3, item4}

	meta2.On("Update", mock.Anything).Return(meta5)

	joined := items.Join(other)

	assert.Len(t, joined, 3)
	assert.True(t, joined[0] == item1)
	assert.True(t, joined[1] == item2)
	assert.True(t, joined[2] == item3)
	assert.True(t, meta5 == item2.Metadata)
	meta2.AssertCalled(t, "Update", meta4)
}

func TestMetastore_PullAll(t *testing.T) {
	p := new(MockProcessor)
	src := new(MockSource)
	meta := new(MockMetadata)
	meta.On("WithOrigin", streams.ProcessorOrigin)
	s := streams.NewMetastore()
	_ = s.Mark(p, src, meta)

	procMeta, err := s.PullAll()

	assert.NoError(t, err)
	assert.Equal(t, map[streams.Processor]streams.Metaitems{p: {{Source: src, Metadata: meta}}}, procMeta)
}

func TestMetastore_PullAllClearsMetastore(t *testing.T) {
	p := new(MockProcessor)
	src := new(MockSource)
	meta := new(MockMetadata)
	meta.On("WithOrigin", streams.ProcessorOrigin)
	s := streams.NewMetastore()
	_ = s.Mark(p, src, meta)

	_, _ = s.PullAll()
	procMeta, err := s.PullAll()

	assert.NoError(t, err)
	assert.Equal(t, map[streams.Processor]streams.Metaitems{}, procMeta)
}

func TestMetastore_Mark(t *testing.T) {
	p := new(MockProcessor)
	src := new(MockSource)
	meta1 := new(MockMetadata)
	meta1.On("WithOrigin", streams.ProcessorOrigin)
	newMeta := new(MockMetadata)
	meta2 := new(MockMetadata)
	meta2.On("WithOrigin", streams.ProcessorOrigin)
	meta2.On("Update", meta1).Return(newMeta)
	s := streams.NewMetastore()

	err := s.Mark(p, src, meta1)
	assert.NoError(t, err)

	err = s.Mark(p, src, meta2)
	assert.NoError(t, err)

	procMeta, _ := s.PullAll()
	assert.Equal(t, map[streams.Processor]streams.Metaitems{p: {{Source: src, Metadata: newMeta}}}, procMeta)
}

func TestMetastore_MarkNilProcessor(t *testing.T) {
	src := new(MockSource)
	meta := new(MockMetadata)
	s := streams.NewMetastore()

	err := s.Mark(nil, src, meta)

	assert.NoError(t, err)
	procMeta, _ := s.PullAll()
	assert.Equal(t, map[streams.Processor]streams.Metaitems{}, procMeta)
}

func TestMetastore_MarkNilSource(t *testing.T) {
	p := new(MockProcessor)
	meta := new(MockMetadata)
	s := streams.NewMetastore()

	err := s.Mark(p, nil, meta)

	assert.NoError(t, err)
	procMeta, _ := s.PullAll()
	assert.Equal(t, map[streams.Processor]streams.Metaitems{}, procMeta)
}

func TestMetastore_MarkNilMetadata(t *testing.T) {
	p := new(MockProcessor)
	src := new(MockSource)
	s := streams.NewMetastore()

	err := s.Mark(p, src, nil)

	assert.NoError(t, err)
	procMeta, _ := s.PullAll()
	assert.Equal(t, map[streams.Processor]streams.Metaitems{}, procMeta)
}

func TestMetastore_MarkMultipleSources(t *testing.T) {
	p := new(MockProcessor)
	src1 := new(MockSource)
	src2 := new(MockSource)
	meta := new(MockMetadata)
	meta.On("WithOrigin", streams.ProcessorOrigin)
	s := streams.NewMetastore()

	err := s.Mark(p, src1, meta)
	assert.NoError(t, err)

	err = s.Mark(p, src2, meta)
	assert.NoError(t, err)

	procMeta, _ := s.PullAll()
	assert.Equal(t, map[streams.Processor]streams.Metaitems{p: {{Source: src1, Metadata: meta}, {Source: src2, Metadata: meta}}}, procMeta)
}

func TestMetastore_Pull(t *testing.T) {
	p := new(MockProcessor)
	src := new(MockSource)
	meta := new(MockMetadata)
	meta.On("WithOrigin", streams.ProcessorOrigin)
	s := streams.NewMetastore()
	_ = s.Mark(p, src, meta)

	pulled, err := s.Pull(p)

	assert.NoError(t, err)
	assert.Equal(t, streams.Metaitems{{Source: src, Metadata: meta}}, pulled)
}

func TestMetastore_PullClearsProcessor(t *testing.T) {
	p := new(MockProcessor)
	src := new(MockSource)
	meta := new(MockMetadata)
	meta.On("WithOrigin", streams.ProcessorOrigin)
	s := streams.NewMetastore()
	_ = s.Mark(p, src, meta)

	_, _ = s.Pull(p)
	pulled, err := s.Pull(p)

	assert.NoError(t, err)
	assert.Equal(t, streams.Metaitems(nil), pulled)
}

func TestMetastore_PullReturnsNilIfDoesntExist(t *testing.T) {
	p := new(MockProcessor)
	s := streams.NewMetastore()

	pulled, err := s.Pull(p)

	assert.NoError(t, err)
	assert.Equal(t, streams.Metaitems(nil), pulled)
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

func (m *fakeMetadata) WithOrigin(streams.MetadataOrigin) {
}

func (m *fakeMetadata) Update(streams.Metadata) streams.Metadata {
	return m
}

func (m *fakeMetadata) Merge(v streams.Metadata) streams.Metadata {
	return m
}
