# Code Quality

## Test Coverage
- Root package has extensive test files for all major components: processor, pipe, pump, supervisor, task, topology, metastore, monitor, message, stream
- Both internal (`_internal_test.go`) and external (`_test.go`) test patterns used
- `kafka/` package has tests for source, sink, and encoder
- `cache/`, `channel/`, `sql/` packages each have corresponding test files
- Coveralls badge present in README (CI-integrated coverage)

## Code Patterns
- **Functional options:** `TaskOptFunc` for configuring Task instances
- **Builder pattern:** `StreamBuilder` and `TopologyBuilder` for constructing topologies
- **Interface-based design:** all core components are interfaces, enabling easy mocking and substitution
- **Decorator pattern:** `TimedSupervisor` wraps `Supervisor` to add auto-commit behavior
- **Null object pattern:** `nullMonitor`, `nullStats` for zero-value defaults
- **Custom nanotime:** assembly-level `nanotime()` for high-resolution timing without allocation

## Error Handling
- Consistent error propagation through return values
- Sentinel errors for known failure states (`ErrNotRunning`, `ErrAlreadyRunning`, `ErrUnknownPump`)
- `ErrorFunc` callback for asynchronous error handling in goroutines (source pumps, async pumps)
- Errors from Source.Consume() are dispatched via goroutine to avoid deadlock

## Documentation
- All exported types and functions have GoDoc comments
- README provides conceptual overview and installation instructions
- Interface contracts are documented inline
- Example applications in `example/` directory demonstrate real usage
