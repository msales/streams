package kafka_test

import (
	"testing"

	"github.com/msales/streams/kafka"
	"github.com/stretchr/testify/assert"
)

func TestByteDecoder_Decode(t *testing.T) {
	in := []byte("foobar")
	dec := kafka.ByteDecoder{}

	got, err := dec.Decode(in)

	assert.NoError(t, err)
	assert.Equal(t, []byte("foobar"), got)
}

func TestByteEncoder_Encode(t *testing.T) {
	tests := []struct {
		in   interface{}
		want []byte
	}{
		{
			in:   []byte("foobar"),
			want: []byte("foobar"),
		},
		{
			in:   nil,
			want: nil,
		},
	}

	for _, tt := range tests {
		enc := kafka.ByteEncoder{}

		got, err := enc.Encode(tt.in)

		assert.NoError(t, err)
		assert.Equal(t, tt.want, got)
	}
}

func TestStringDecoder_Decode(t *testing.T) {
	in := []byte("foobar")
	dec := kafka.StringDecoder{}

	got, err := dec.Decode(in)

	assert.NoError(t, err)
	assert.Equal(t, "foobar", got)
}

func TestStringEncoder_Encode(t *testing.T) {
	tests := []struct {
		in   interface{}
		want []byte
	}{
		{
			in:   "foobar",
			want: []byte("foobar"),
		},
		{
			in:   nil,
			want: nil,
		},
	}

	for _, tt := range tests {
		enc := kafka.StringEncoder{}

		got, err := enc.Encode(tt.in)

		assert.NoError(t, err)
		assert.Equal(t, tt.want, got)
	}
}
