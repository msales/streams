package streams

// Source represents a stream source.
type Source interface {
	// WithContext sets the context on the Source.
	WithContext(ctx Context)
	// Consume gets the next record from the Source.
	Consume() (key, value interface{}, err error)
	// Commit marks the consumed records as processed.
	Commit() error
	// Close closes the Source.
	Close() error
}
