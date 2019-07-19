package streams

import (
	"context"
)

// MetadataOrigin represents the metadata origin type.
type MetadataOrigin uint8

// MetadataOrigin types.
const (
	CommitterOrigin MetadataOrigin = iota
	ProcessorOrigin
)

// MetadataStrategy represents the metadata merge strategy.
type MetadataStrategy uint8

// MetadataStrategy types.
const (
	Lossless MetadataStrategy = iota
	Dupless
)

// Metadata represents metadata that can be merged.
type Metadata interface {
	// WithOrigin sets the MetadataOrigin on the metadata.
	WithOrigin(MetadataOrigin)
	// Merge merges the contained metadata into the given the metadata with the given strategy.
	Merge(Metadata, MetadataStrategy) Metadata
}

// Message represents data the flows through the stream.
type Message struct {
	source   Source
	metadata Metadata

	Ctx   context.Context
	Key   interface{}
	Value interface{}
}

// Metadata returns the Message Metadata.
func (m Message) Metadata() (Source, Metadata) {
	return m.source, m.metadata
}

// WithMetadata add metadata to the Message for a Source.
func (m Message) WithMetadata(s Source, v Metadata) Message {
	m.source = s
	m.metadata = v

	return m
}

// Empty determines if the Message is empty.
func (m Message) Empty() bool {
	return m.Key == nil && m.Value == nil
}

// EmptyMessage is a predefined empty message.
var EmptyMessage = Message{}

// NewMessage creates a Message.
func NewMessage(k, v interface{}) Message {
	return Message{
		source:   nil,
		metadata: nil,
		Ctx:      context.Background(),
		Key:      k,
		Value:    v,
	}
}

// NewMessageWithContext creates a Message with the given context.
func NewMessageWithContext(ctx context.Context, k, v interface{}) Message {
	return Message{
		source:   nil,
		metadata: nil,
		Ctx:      ctx,
		Key:      k,
		Value:    v,
	}
}
