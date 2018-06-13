package streams

import (
	"context"
)

type Message struct {
	Ctx   context.Context
	Key   interface{}
	Value interface{}
}

func (m Message) Empty() bool {
	return m.Key == nil && m.Value == nil
}

func NewMessage(k, v interface{}) *Message {
	return &Message{
		Ctx:   context.Background(),
		Key:   k,
		Value: v,
	}
}

func NewMessageWithContext(ctx context.Context, k, v interface{}) *Message {
	return &Message{
		Ctx:   ctx,
		Key:   k,
		Value: v,
	}
}
