package streams

// Supervisor represents a commit supervisor.
//
// The Supervisor performs a commit in a concurrently-safe manner.
// There can only ever be 1 ongoing commit at any given time.
type Supervisor interface {
	// Perform a global commit sequence.
	//
	// If triggered by a Node, that node should be passed as an argument to Commit.
	Commit(Processor) error
}

type sourceMetadata map[Source]Metadata

// Merge merges metadata from metaitems.
//
// TODO: Incorporate merge strategies.
func (m sourceMetadata) Merge(items []Metaitem) sourceMetadata {
	for _, item := range items {
		m[item.Source] = item.Metadata.Merge(m[item.Source])
	}
	return m
}

// Compile-type interface check.
var _ Supervisor = (*supervisor)(nil)

type supervisor struct {
	store Metastore

	pumps map[Processor]Pump

	committerMeta []Metaitem
	markerMeta    []Metaitem
}

// NewSupervisor returns a new Supervisor instance.
func NewSupervisor(store Metastore, pumps map[Node]Pump) Supervisor {
	return &supervisor{
		store: store,

		pumps: remapPumps(pumps),
	}
}

// Perform a global commit sequence.
//
// TODO: ensure that a Commit cannot be called when another Commit is in progress.
func (s *supervisor) Commit(p Processor) error {
	metadata, err := s.store.PullAll()
	if err != nil {
		return err
	}

	// 1. Process all processors that have submitted any metadata.
	for proc, items := range metadata {
		err := s.commit(proc, items)
		if err != nil {
			return err
		}
	}

	// 2. Merge all metadata and group by source.
	srcMeta := make(sourceMetadata).
		Merge(s.committerMeta).
		Merge(s.markerMeta)

	// 3. Commit the merged metadata on each source.
	for src, meta := range srcMeta {
		err := src.Commit(meta)
		if err != nil {
			return err
		}
	}

	s.reset()

	return nil
}

func (s *supervisor) commit(proc Processor, items []Metaitem) error {
	var updatedItems []Metaitem

	if cmt, ok := proc.(Committer); ok {
		s.committerMeta = append(s.committerMeta, items...)

		err := s.pumps[proc].WithLock(func() error {
			err := cmt.Commit()
			if err != nil {
				return err
			}

			updatedItems, err = s.store.Pull(proc)

			return err
		})
		if err != nil {
			return err
		}

		s.committerMeta = append(s.committerMeta, updatedItems...)

		return nil
	}

	s.markerMeta = append(s.markerMeta, items...)

	return nil
}

func (s *supervisor) reset() {
	s.committerMeta = s.committerMeta[:0]
	s.markerMeta = s.markerMeta[:0]
}

func remapPumps(pumps map[Node]Pump) map[Processor]Pump {
	mapped := make(map[Processor]Pump, len(pumps))
	for node, pump := range pumps {
		mapped[node.Processor()] = pump
	}
	return mapped
}
