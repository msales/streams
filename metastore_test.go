package streams_test

import (
	"testing"

	"github.com/msales/streams/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestMetaitems_MergeDupless(t *testing.T) {
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

	meta2.On("Merge", mock.Anything, mock.Anything).Return(meta5)

	joined := items.Merge(other, streams.Dupless)

	assert.Len(t, joined, 3)
	assert.True(t, joined[0] == item1)
	assert.True(t, joined[1] == item2)
	assert.True(t, joined[2] == item3)
	assert.True(t, meta5 == item2.Metadata)
	meta2.AssertCalled(t, "Merge", meta4, streams.Dupless)
}

func TestMetaitems_MergeLossless(t *testing.T) {
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

	meta2.On("Merge", mock.Anything, mock.Anything).Return(meta5)

	merged := items.Merge(other, streams.Lossless)

	assert.Len(t, merged, 3)
	assert.True(t, merged[0] == item1)
	assert.True(t, merged[1] == item2)
	assert.True(t, merged[2] == item3)
	assert.True(t, meta5 == item2.Metadata)
	meta2.AssertCalled(t, "Merge", meta4, streams.Lossless)
}

func TestMetaitems_MergeHandlesNilSourceAndMetadata(t *testing.T) {
	items := streams.Metaitems{{Source: nil, Metadata: nil}}
	other := streams.Metaitems{{Source: nil, Metadata: nil}}

	joined := items.Merge(other, streams.Lossless)

	assert.Len(t, joined, 1)
}

func BenchmarkMetaitems_Merge(b *testing.B) {
	src1 := &fakeSource{}
	src2 := &fakeSource{}
	src3 := &fakeSource{}

	meta1 := &fakeMetadata{}
	meta2 := &fakeMetadata{}
	meta3 := &fakeMetadata{}
	meta4 := &fakeMetadata{}

	items1 := streams.Metaitems{{Source: src1, Metadata: meta1}, {Source: src2, Metadata: meta2}}
	items2 := streams.Metaitems{{Source: src3, Metadata: meta3}, {Source: src2, Metadata: meta4}}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		items1.Merge(items2, streams.Lossless)
	}
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

func TestMetastore_MarkProcessor(t *testing.T) {
	p := new(MockProcessor)
	src := new(MockSource)
	meta1 := new(MockMetadata)
	meta1.On("WithOrigin", streams.ProcessorOrigin)
	newMeta := new(MockMetadata)
	meta2 := new(MockMetadata)
	meta2.On("WithOrigin", streams.ProcessorOrigin)
	meta2.On("Merge", meta1, streams.Dupless).Return(newMeta)
	s := streams.NewMetastore()

	err := s.Mark(p, src, meta1)
	assert.NoError(t, err)

	err = s.Mark(p, src, meta2)
	assert.NoError(t, err)

	procMeta, _ := s.PullAll()
	assert.Equal(t, map[streams.Processor]streams.Metaitems{p: {{Source: src, Metadata: newMeta}}}, procMeta)
}

func TestMetastore_MarkCommitter(t *testing.T) {
	p := new(MockCommitter)
	src := new(MockSource)
	meta1 := new(MockMetadata)
	meta1.On("WithOrigin", streams.CommitterOrigin)
	newMeta := new(MockMetadata)
	meta2 := new(MockMetadata)
	meta2.On("WithOrigin", streams.CommitterOrigin)
	meta2.On("Merge", meta1, streams.Dupless).Return(newMeta)
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

	err = s.Mark(nil, src, meta)
	assert.NoError(t, err)

	procMeta, _ := s.PullAll()
	assert.Equal(t, map[streams.Processor]streams.Metaitems{}, procMeta)
}

func TestMetastore_MarkNilSource(t *testing.T) {
	p := new(MockProcessor)
	meta := new(MockMetadata)
	meta.On("WithOrigin", streams.ProcessorOrigin)
	s := streams.NewMetastore()

	err := s.Mark(p, nil, meta)
	assert.NoError(t, err)

	err = s.Mark(p, nil, meta)
	assert.NoError(t, err)

	procMeta, _ := s.PullAll()
	assert.Equal(t, map[streams.Processor]streams.Metaitems{p: {{Source: nil, Metadata: meta}}}, procMeta)
}

func TestMetastore_MarkNilMetadata(t *testing.T) {
	p := new(MockProcessor)
	src := new(MockSource)
	s := streams.NewMetastore()

	err := s.Mark(p, src, nil)
	assert.NoError(t, err)

	err = s.Mark(p, src, nil)
	assert.NoError(t, err)

	procMeta, _ := s.PullAll()
	assert.Equal(t, map[streams.Processor]streams.Metaitems{p: {{Source: src, Metadata: nil}}}, procMeta)
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
