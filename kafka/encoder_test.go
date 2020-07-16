package kafka_test

import (
	"errors"
	"testing"

	"github.com/msales/streams/v6/kafka"
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

func TestDecoderFunc_Decode(t *testing.T) {
	b := []byte("payload")
	e := errors.New("test")
	i := interface{}("entity")

	f := func(value []byte) (interface{}, error) {
		assert.Equal(t, b, value)

		return i, e
	}

	decoder := kafka.DecoderFunc(f)
	result, err := decoder.Decode(b)

	assert.True(t, i == result, "Received object is not exactly the same object that was returned by the function.")
	assert.True(t, e == err, "Received error is not exactly the same object that was returned by the function.")
}

func TestEncoderFunc_Encode(t *testing.T) {
	b := []byte("payload")
	e := errors.New("test")
	i := interface{}("entity")

	f := func(object interface{}) ([]byte, error) {
		assert.True(t, i == object, "Received object is not exactly the same as one that was that passed to the encoder.")

		return b, e
	}

	encoder := kafka.EncoderFunc(f)
	result, err := encoder.Encode(i)

	assert.Equal(t, b, result)
	assert.True(t, e == err, "Received error is not exactly the same object that was returned by the function.")
}

func TestNilDecoder(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
	}{
		{name: "some bytes", in: []byte("test")},
		{name: "empty bytes", in: []byte{}},
		{name: "nil", in: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := kafka.NilDecoder.Decode(tt.in)

			assert.NoError(t, err)
			assert.Nil(t, got)
		})
	}
}
