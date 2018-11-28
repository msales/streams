package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMergedMetadata_Merge(t *testing.T) {
	a := MergedMetadata{0: 3}
	b := MergedMetadata{1: 2}

	res := b.Merge(a)

	assert.IsType(t, MergedMetadata{}, res)
	a = res.(MergedMetadata)
	assert.Equal(t, MergedMetadata{0: 3, 1: 2}, a)
}

func TestMergedMetadata_MergePicksLowest(t *testing.T) {
	a := MergedMetadata{0: 10}
	b := MergedMetadata{0: 2}

	res := b.Merge(a)

	assert.IsType(t, MergedMetadata{}, res)
	a = res.(MergedMetadata)
	assert.Equal(t, MergedMetadata{0: 2}, a)
}

func TestMergedMetadata_MergeNilMerged(t *testing.T) {
	b := MergedMetadata{0: 5}

	res := b.Merge(nil)

	assert.IsType(t, MergedMetadata{}, res)
	a := res.(MergedMetadata)
	assert.Equal(t, MergedMetadata{0: 5}, a)
}

func TestMetadata_Merge(t *testing.T) {
	merged := MergedMetadata{0: 3, 1: 10}
	meta := &Metadata{Topic: "foo", Partition: 1, Offset: 2}

	res := meta.Merge(merged)

	assert.IsType(t, MergedMetadata{}, res)
	merged = res.(MergedMetadata)
	assert.Equal(t, MergedMetadata{0: 3, 1: 2}, merged)
}

func TestMetadata_MergePicksLowest(t *testing.T) {
	merged := MergedMetadata{0: 3}
	meta := &Metadata{Topic: "foo", Partition: 0, Offset: 10}

	res := meta.Merge(merged)

	assert.IsType(t, MergedMetadata{}, res)
	merged = res.(MergedMetadata)
	assert.Equal(t, MergedMetadata{0: 3}, merged)
}

func TestMetadata_MergeNilMerged(t *testing.T) {
	meta := &Metadata{Topic: "baz", Partition: 1, Offset: 10}

	res := meta.Merge(nil)

	assert.IsType(t, MergedMetadata{}, res)
	merged := res.(MergedMetadata)
	assert.Equal(t, MergedMetadata{1: 10}, merged)
}