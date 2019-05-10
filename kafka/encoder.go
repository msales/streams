package kafka

// NilDecoder is a decoder that always returns a nil, no matter the input.
var NilDecoder DecoderFunc = func([]byte) (interface{}, error) { return nil, nil }

// Decoder represents a Kafka data decoder.
type Decoder interface {
	// Decode transforms byte data to the desired type.
	Decode([]byte) (interface{}, error)
}

// DecoderFunc is an adapter allowing to use a function as a decoder.
type DecoderFunc func(value []byte) (interface{}, error)

// Decode transforms byte data to the desired type.
func (f DecoderFunc) Decode(value []byte) (interface{}, error) {
	return f(value)
}

// Encoder represents a Kafka data encoder.
type Encoder interface {
	// Encode transforms the typed data to bytes.
	Encode(interface{}) ([]byte, error)
}

// EncoderFunc is an adapter allowing to use a function as an encoder.
type EncoderFunc func(interface{}) ([]byte, error)

// Encode transforms the typed data to bytes.
func (f EncoderFunc) Encode(value interface{}) ([]byte, error) {
	return f(value)
}

// ByteDecoder represents a byte decoder.
type ByteDecoder struct{}

// Decode transforms byte data to the desired type.
func (d ByteDecoder) Decode(b []byte) (interface{}, error) {
	return b, nil
}

// ByteEncoder represents a byte encoder.
type ByteEncoder struct{}

// Encode transforms the typed data to bytes.
func (e ByteEncoder) Encode(v interface{}) ([]byte, error) {
	if v == nil {
		return nil, nil
	}

	return v.([]byte), nil
}

// StringDecoder represents a string decoder.
type StringDecoder struct{}

// Decode transforms byte data to a string.
func (d StringDecoder) Decode(b []byte) (interface{}, error) {
	return string(b), nil
}

// StringEncoder represents a string encoder.
type StringEncoder struct{}

// Encode transforms the string data to bytes.
func (e StringEncoder) Encode(v interface{}) ([]byte, error) {
	if v == nil {
		return nil, nil
	}

	return []byte(v.(string)), nil
}
