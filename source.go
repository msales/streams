package streams

type Source interface {
	Consume() (key, value interface{}, err error)
	Commit() error
	Close() error
}
