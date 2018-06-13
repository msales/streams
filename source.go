package streams

// Source represents a stream source.
type Source interface {
	// Consume gets the next Message from the Source.
	Consume() (*Message, error)
	// Commit marks the consumed Message as processed.
	Commit() error
	// Close closes the Source.
	Close() error
}
