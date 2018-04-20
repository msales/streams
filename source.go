package streams

type Source interface {
	Consume() (key, value interface{}, err error)
	Commit(sync bool) error
	Close() error
}
