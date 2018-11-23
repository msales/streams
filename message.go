package streams

import (
	"context"
)

// Message represents data the flows through the stream.
type Message struct {
	source   Source
	metadata interface{}

	Ctx   context.Context
	Key   interface{}
	Value interface{}
}

// Metadata returns the Message metadata.
func (m *Message) Metadata() (Source, interface{}) {
	return m.source, m.metadata
}

// WithMetadata add metadata to the Message from a Source.
func (m *Message) WithMetadata(s Source, v interface{}) *Message {
	m.source = s
	m.metadata = v

	return m
}

// Empty determines if the Message is empty.
func (m Message) Empty() bool {
	return m.Key == nil && m.Value == nil
}

// NewMessage creates a Message.
func NewMessage(k, v interface{}) *Message {
	return &Message{
		metadata: map[Source]interface{}{},
		Ctx:      context.Background(),
		Key:      k,
		Value:    v,
	}
}

// NewMessageWithContext creates a Message with the given context.
func NewMessageWithContext(ctx context.Context, k, v interface{}) *Message {
	return &Message{
		metadata: map[Source]interface{}{},
		Ctx:      ctx,
		Key:      k,
		Value:    v,
	}
}
