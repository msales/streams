package kafka

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/xdg/scram"

	"github.com/msales/streams/v6"
)

// CommitStrategy represents commit strategy for source commiting.
type CommitStrategy int

const (
	// CommitAuto represents automatic commit strategy.
	// It takes advantage of Shopify/sarama's AutoCommit.
	CommitAuto CommitStrategy = 0

	// CommitManual represents manual commit strategy. Commiting is done on Commit method in Source.
	// It turns off Shopify/sarama's AutoCommit by default.
	CommitManual CommitStrategy = 1

	// CommitBoth represents commit strategy that uses both CommitAuto and CommitManual.
	// Commiting is done using AutoCommit and on Commit method in Source
	CommitBoth CommitStrategy = 2
)

// SourceConfig represents the configuration for a Kafka stream source.
type SourceConfig struct {
	sarama.Config

	Brokers []string
	Topic   string
	GroupID string

	Ctx          context.Context
	KeyDecoder   Decoder
	ValueDecoder Decoder

	BufferSize       int
	ErrorsBufferSize int

	CommitStrategy CommitStrategy
}

// NewSourceConfig creates a new Kafka source configuration.
func NewSourceConfig() *SourceConfig {
	c := &SourceConfig{
		Config: *sarama.NewConfig(),
	}

	c.Ctx = context.Background()
	c.KeyDecoder = ByteDecoder{}
	c.ValueDecoder = ByteDecoder{}
	c.BufferSize = 1000
	c.ErrorsBufferSize = 10
	c.Consumer.Return.Errors = true

	return c
}

// Validate checks a Config instance. It will return a
// sarama.ConfigurationError if the specified values don't make sense.
func (c *SourceConfig) Validate() error {
	if err := c.Config.Validate(); err != nil {
		return err
	}

	switch {
	case c.Brokers == nil || len(c.Brokers) == 0:
		return sarama.ConfigurationError("Brokers must have at least one broker")
	case c.KeyDecoder == nil:
		return sarama.ConfigurationError("KeyDecoder must be an instance of Decoder")
	case c.ValueDecoder == nil:
		return sarama.ConfigurationError("ValueDecoder must be an instance of Decoder")
	case c.BufferSize <= 0:
		return sarama.ConfigurationError("BufferSize must be at least 1")
	}

	return nil
}

// ModifyConfig modifies config.
func (c *SourceConfig) ModifyConfig() {
	if c.CommitStrategy == CommitManual {
		c.Config.Consumer.Offsets.AutoCommit.Enable = false
	}
}

// Compile-time check.
var _ sarama.SCRAMClient = (*SCRAMClient)(nil)

// SCRAMClient represents a SASL/SCRAM client.
type SCRAMClient struct {
	*scram.Client
	*scram.ClientConversation

	hashGenerator scram.HashGeneratorFcn
}

// NewSCRAMClientGeneratorFn is a SCRAM client generator function for sarama.Config.
func NewSCRAMClientGeneratorFn(hashFn scram.HashGeneratorFcn) func() sarama.SCRAMClient {
	return func() sarama.SCRAMClient {
		return &SCRAMClient{hashGenerator: hashFn}
	}
}

func (x *SCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.hashGenerator.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}

	x.ClientConversation = x.Client.NewConversation()

	return nil
}

func (x *SCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)

	return
}

func (x *SCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}

// Metadata represents an the kafka topic metadata.
type Metadata []*PartitionOffset

func (m Metadata) find(topic string, partition int32) (int, *PartitionOffset) {
	for i, pos := range m {
		if pos.Topic == topic && pos.Partition == partition {
			return i, pos
		}
	}

	return -1, nil
}

// WithOrigin sets the MetadataOrigin on the metadata.
func (m Metadata) WithOrigin(o streams.MetadataOrigin) {
	for _, pos := range m {
		pos.Origin = o
	}
}

// Merge merges the contained metadata into the given the metadata.
func (m Metadata) Merge(v streams.Metadata, s streams.MetadataStrategy) streams.Metadata {
	if v == nil {
		return m
	}

	metadata := v.(Metadata)
	for _, newPos := range m {
		i, oldPos := metadata.find(newPos.Topic, newPos.Partition)
		if oldPos == nil {
			metadata = append(metadata, newPos)
			continue
		}

		if newPos.Origin > oldPos.Origin {
			continue
		}

		if newPos.Origin < oldPos.Origin {
			metadata[i] = newPos
		}

		// At this point origins are equal
		if (s == streams.Lossless && newPos.Offset < oldPos.Offset) ||
			(s == streams.Dupless && newPos.Offset > oldPos.Offset) {
			metadata[i] = newPos
		}
	}

	return metadata
}

// PartitionOffset represents the position in the stream of a message.
type PartitionOffset struct {
	Origin streams.MetadataOrigin

	Topic     string
	Partition int32
	Offset    int64
}

// Source represents a Kafka stream source.
type Source struct {
	lastErr  error
	errs     chan error
	done     chan struct{}
	messages chan *sarama.ConsumerMessage

	ctx    context.Context
	cancel context.CancelFunc

	client    sarama.ConsumerGroup
	consumeWG *sync.WaitGroup
	consumer  *consumerHandler
	tracker   *consumptionTracker

	commitStrategy CommitStrategy
	keyDecoder     Decoder
	valueDecoder   Decoder
}

// NewSource creates a new Kafka stream source.
func NewSource(c *SourceConfig) (*Source, error) {
	c.ModifyConfig()
	if err := c.Validate(); err != nil {
		return nil, err
	}

	messagesCh := make(chan *sarama.ConsumerMessage, c.BufferSize)
	doneCh := make(chan struct{})

	tracker := newConsumptionTracker()
	consumer := newConsumerHandler(messagesCh, doneCh, tracker)
	client, err := sarama.NewConsumerGroup(c.Brokers, c.GroupID, &c.Config)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(c.Ctx)
	s := &Source{
		done:     doneCh,
		errs:     make(chan error, c.ErrorsBufferSize),
		messages: messagesCh,

		ctx:    ctx,
		cancel: cancel,

		client:    client,
		consumer:  consumer,
		tracker:   tracker,
		consumeWG: &sync.WaitGroup{},

		commitStrategy: c.CommitStrategy,
		keyDecoder:     c.KeyDecoder,
		valueDecoder:   c.ValueDecoder,
	}

	s.consumeWG.Add(1)

	go s.startConsumption(ctx, c.Topic, consumer)
	go s.readErrors()

	consumer.readyWG.Wait()   // Await till the consumer has been set up
	consumer.sessionWG.Done() // Indicate that session is ready to be used.
	return s, nil
}

// Consume gets the next record from the Source.
func (s *Source) Consume() (streams.Message, error) {
	if s.lastErr != nil {
		return streams.EmptyMessage, s.lastErr
	}

	select {
	case msg := <-s.messages:
		k, err := s.keyDecoder.Decode(msg.Key)
		if err != nil {
			return streams.EmptyMessage, err
		}

		v, err := s.valueDecoder.Decode(msg.Value)
		if err != nil {
			return streams.EmptyMessage, err
		}

		m := streams.NewMessageWithContext(s.ctx, k, v).
			WithMetadata(s, s.createMetadata(msg))
		return m, nil

	case <-time.After(100 * time.Millisecond):
		return streams.EmptyMessage, nil
	}
}

// Commit marks the consumed records as processed.
func (s *Source) Commit(v interface{}) error {
	if v == nil {
		return nil
	}

	// Lock the session for the duration of the commit.
	session := s.consumer.LockSession()
	defer s.consumer.FreeSession()

	if session == nil { // May still happen, although it's a very slim chance.
		return errors.New("kafka: consumer session was closed or doesn't exist")
	}

	state := v.(Metadata)
	for _, pos := range state {
		// This function does not guarantee immediate commit (efficiency reasons). Therefore it is possible
		// that the offsets are never committed if the application crashes. This may lead to double-committing
		// on rare occasions.
		// The result of the "Commit" method on the Committer should be idempotent whenever possible!
		//
		// If offsets are needed to be committed immediately, use CommitManual or CommitBoth.
		session.MarkOffset(pos.Topic, pos.Partition, pos.Offset+1, "")
		s.tracker.MarkCommitted(pos.Partition, pos.Offset)
	}

	// If commit strategy is not CommitAuto, session should perform global, synchronous commit of current marked offsets.
	if s.commitStrategy != CommitAuto {
		session.Commit()
		runtime.Gosched() // If any error from consumer side happens after Commiting, it will be read and s.lastErr will be set.
	}

	return s.lastErr
}

// Close closes the Source.
func (s *Source) Close() error {
	s.tracker.Close()
	close(s.done)
	s.cancel()
	s.consumeWG.Wait()

	return s.client.Close()
}

func (s *Source) createMetadata(msg *sarama.ConsumerMessage) Metadata {
	return Metadata{&PartitionOffset{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	}}
}

func (s *Source) startConsumption(ctx context.Context, topic string, consumer *consumerHandler) {
	defer s.consumeWG.Done()

	for {
		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		err := s.client.Consume(s.ctx, []string{topic}, consumer)
		if errors.Is(ctx.Err(), context.Canceled) { // This is the proper way to end the consumption.
			return
		}

		if err == nil {
			err = ctx.Err()
		}

		if err != nil {
			s.errs <- err
			consumer.readyWG.Done()
			return
		}

		// This will happen AFTER Setup function is called.
		// Therefore, readyWG has to be initiated before calling Consume for the first time.
		// After that it will work as expected.
		consumer.readyWG.Add(1)
	}
}

func (s *Source) readErrors() {
	for {
		select {
		case <-s.done:
			return
		case err := <-s.errs:
			s.lastErr = err
		case err := <-s.client.Errors():
			s.lastErr = err
		}
	}
}

// consumerHandler contains implementation of sarama.ConsumerGroupHandler.
// It controls the lifecycle of the consumer group session and passes claimed messages to the Source.
// How the lifecycle works:
//  1. When a new session is started, Setup is called. It sets the session in the handler and
//     prepares the handler for message consumption.
//  2. ConsumeClaim is called for each claim (partition) assigned to the consumer. It reads messages
//     from the claim and sends them to the Source via the messages channel.
//  3. When a rebalance is triggered, Cleanup is called. It waits for all messages from the current session
//     to be processed and committed before allowing the session to be cleaned up.
//  4. The Source processes messages and commits offsets using the Commit method.
//  5. If there are no more messages in the buffer and the session is being cleaned up, it signals
//     the handler to unlock the session cleanup.
type consumerHandler struct {
	readyWG   *sync.WaitGroup
	sessionWG *sync.WaitGroup
	messages  chan *sarama.ConsumerMessage
	done      chan struct{}

	tracker *consumptionTracker

	session     sarama.ConsumerGroupSession
	sessionLock sync.RWMutex
}

func newConsumerHandler(messages chan *sarama.ConsumerMessage, done chan struct{}, tracker *consumptionTracker) *consumerHandler {
	ch := &consumerHandler{
		tracker:  tracker,
		messages: messages,
		done:     done,

		readyWG:   &sync.WaitGroup{},
		sessionWG: &sync.WaitGroup{},
	}

	// Initially we are waiting for the first session to be ready.
	ch.readyWG.Add(1)
	ch.sessionWG.Add(1)

	return ch
}

func (c *consumerHandler) LockSession() sarama.ConsumerGroupSession {
	c.sessionWG.Add(1)
	c.sessionLock.Lock()

	return c.session
}

func (c *consumerHandler) FreeSession() {
	c.sessionLock.Unlock()
	c.sessionWG.Done()
}

func (c *consumerHandler) Setup(session sarama.ConsumerGroupSession) error {
	c.sessionLock.Lock()
	c.session = session
	c.sessionLock.Unlock()

	c.readyWG.Done()

	return nil
}

func (c *consumerHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	// Wait till all messages from the session are processed and committed.
	c.tracker.Wait()

	c.sessionWG.Wait() // Wait till any ongoing session access is done.
	c.sessionLock.Lock()
	defer c.sessionLock.Unlock()
	c.session = nil

	return nil
}

func (c *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/IBM/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			c.tracker.MarkConsumed(message.Partition, message.Offset)
			select {
			case c.messages <- message:
			// This is to avoid deadlocking during shutdown in a case where:
			// - the pumps are not running anymore
			// - s.buf is full (but not draining, since pumps are off)
			// - we have consumed a message and are attempting to send it to s.buf
			case <-c.done:
			}
		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}

// consumptionTracker tracks consumed offsets and allows waiting until they have been committed.
// It's whole responsibility appeared when we discovered that session is overridden.
// Due to the lifecycle of sarama.ConsumerGroupSession and how msales/streams is handling commits
// we had to introduce a way to track claimed messages and make sure that they have been committed
// once Cleanup of the session is in progress.
type consumptionTracker struct {
	isClosing bool

	mu sync.RWMutex
	wg *sync.WaitGroup
	// partition -> offset
	offsets map[int32]int64
}

func newConsumptionTracker() *consumptionTracker {
	return &consumptionTracker{
		offsets: make(map[int32]int64),
		wg:      &sync.WaitGroup{},
	}
}

// Close blocks until all consumed offsets have been committed.
func (t *consumptionTracker) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.isClosing = true
}

// Wait blocks until there is at least one consumed offset being tracked.
func (t *consumptionTracker) Wait() {
	t.mu.RLock()
	isClosing := t.isClosing
	t.mu.RUnlock()
	if isClosing {
		return
	}

	t.wg.Wait()
}

// HasOffsets returns true if there are no offsets being tracked.
func (t *consumptionTracker) HasOffsets() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return len(t.offsets) != 0
}

// MarkConsumed marks the given partition and offset as consumed.
func (t *consumptionTracker) MarkConsumed(partition int32, offset int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.offsets[partition]; !ok {
		// First time seeing this partition, increment the wait group.
		t.wg.Add(1)
	}

	t.offsets[partition] = offset
}

// MarkCommitted marks the given partition and offset as committed.
func (t *consumptionTracker) MarkCommitted(partition int32, offset int64) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	currentOffset, ok := t.offsets[partition]
	if !ok {
		return false
	}

	// We only track the latest offset for each partition, so if the committed offset is less than
	// the current offset, we ignore it.
	// This can happen if somehow claimed message with offset 150 is processed before claimed message with offset 180.
	if offset < currentOffset {
		return false
	}

	// Remove the partition tracking as it has been committed.
	delete(t.offsets, partition)
	t.wg.Done()

	return true
}
