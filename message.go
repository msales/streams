package streams

import (
	"context"
)

type Message struct {
	metadata map[Source]interface{}

	Ctx   context.Context
	Key   interface{}
	Value interface{}
}

func (m *Message) Metadata() map[Source]interface{} {
	return m.metadata
}

func (m *Message) WithMetadata(s Source, v interface{}) *Message {
	m.metadata[s] = v

	return m
}

func (m Message) Empty() bool {
	return m.Key == nil && m.Value == nil
}

func NewMessage(k, v interface{}) *Message {
	return &Message{
		metadata: map[Source]interface{}{},
		Ctx:      context.Background(),
		Key:      k,
		Value:    v,
	}
}

func NewMessageWithContext(ctx context.Context, k, v interface{}) *Message {
	return &Message{
		metadata: map[Source]interface{}{},
		Ctx:      ctx,
		Key:      k,
		Value:    v,
	}
}

type NodeMessage struct {
	Node Node
	Msg  *Message
}
