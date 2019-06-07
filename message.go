package streams

import (
	"context"
	"runtime"
	"sync/atomic"
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

type MessageBuffer struct {
	head int64
	tail int64
	len  int64
	size int64
	data []Message
	mask int64
	done int32
}

func NewMessageBuffer(size int) *MessageBuffer {
	buf := &MessageBuffer{}
	buf.data = make([]Message, roundUp2(uint32(size)))
	buf.mask = int64(len(buf.data) - 1)
	buf.size = int64(size)

	return buf
}

func (b *MessageBuffer) Write(msg Message) {
	head := b.head
	if l := atomic.LoadInt64(&b.len); l >= b.size {
		for ; l >= b.size; l = atomic.LoadInt64(&b.len) {
			b.wait()
		}
	}

	b.data[head] = msg
	atomic.AddInt64(&b.len, 1)
	atomic.StoreInt64(&b.head, (head+1)&b.mask)
}

func (b *MessageBuffer) Read(p []Message) int {
	pl := len(p)
	if pl > int(b.size) {
		pl = int(b.size)
	}
	tail := b.tail
	count := 0

	for {
		l := atomic.LoadInt64(&b.len)
		for l == 0 {
			// If we have read at least some data or are closed, dont wait, just return what we read
			if atomic.LoadInt32(&b.done) > 0 || count > 0 {
				return count
			}
			b.wait()
			l = atomic.LoadInt64(&b.len)
		}

		i := int64(0)
		for ; l > 0 && count < pl; count++ {
			p[count] = b.data[tail]
			tail = (tail + 1) & b.mask
			i++
			l--
		}

		atomic.StoreInt64(&b.tail, tail)
		atomic.AddInt64(&b.len, -i)

		if count == pl {
			return count
		}
	}
}

func (b *MessageBuffer) wait() {
	runtime.Gosched()
}

func (b *MessageBuffer) Done() bool {
	return atomic.LoadInt64(&b.len) == 0 && atomic.LoadInt32(&b.done) > 0
}

func (b *MessageBuffer) Len() int {
	return int(atomic.LoadInt64(&b.len))
}

func (b *MessageBuffer) Cap() int {
	return int(b.size)
}

func (b *MessageBuffer) Close() {
	atomic.AddInt32(&b.done, 1)
}

func roundUp2(v uint32) uint32 {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	return v + 1
}
