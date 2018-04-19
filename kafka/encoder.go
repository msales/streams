package kafka

type Decoder interface {
	Decode([]byte) (interface{}, error)
}

type Encoder interface {
	Encode(interface{}) ([]byte, error)
}

type ByteDecoder struct{}

func (d ByteDecoder) Decode(b []byte) (interface{}, error) {
	return b, nil
}

type ByteEncoder struct{}

func (e ByteEncoder) Encode(v interface{}) ([]byte, error) {
	if v == nil {
		return nil, nil
	}

	return v.([]byte), nil
}

type StringDecoder struct{}

func (d StringDecoder) Decode(b []byte) (interface{}, error) {
	return string(b), nil
}

type StringEncoder struct{}

func (e StringEncoder) Encode(v interface{}) ([]byte, error) {
	if v == nil {
		return nil, nil
	}

	return []byte(v.(string)), nil
}
