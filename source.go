package streams

// Source represents a stream source.
type Source interface {
	// Consume gets the next record from the Source.
	Consume() (*Message, error)
	// Commit marks the consumed records as processed.
	Commit() error
	// Close closes the Source.
	Close() error
}
