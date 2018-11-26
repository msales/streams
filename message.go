package streams

import (
	"context"
)

// Metadata represents metadata that can be merged.
type Metadata interface {
	// Merge merges two pieces of metadata.
	Merge(interface{}) interface{}
}

// Message represents data the flows through the stream.
type Message struct {
	source   Source
	metadata Metadata

	Ctx   context.Context
	Key   interface{}
	Value interface{}
}

// Metadata returns the Message metadata.
func (m *Message) Metadata() (Source, interface{}) {
	return m.source, m.metadata
}

// WithMetadata add metadata to the Message from a Source.
func (m *Message) WithMetadata(s Source, v Metadata) *Message {
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
		source:   nil,
		metadata: nil,
		Ctx:      context.Background(),
		Key:      k,
		Value:    v,
	}
}

// NewMessageWithContext creates a Message with the given context.
func NewMessageWithContext(ctx context.Context, k, v interface{}) *Message {
	return &Message{
		source:   nil,
		metadata: nil,
		Ctx:      ctx,
		Key:      k,
		Value:    v,
	}
}
