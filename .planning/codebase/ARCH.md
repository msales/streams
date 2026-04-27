# Architecture

## Overview
Streams is a stream processing library that models data flow as a directed acyclic graph (DAG) of Sources, Processors, and Pipes. It provides a builder API (`StreamBuilder`) to construct a `Topology`, which is then executed by a `Task` with configurable concurrency (sync/async pumps) and commit strategies.

## Package Structure
| Package | Responsibility |
|---|---|
| `streams` (root) | Core abstractions: Message, Source, Processor, Pipe, Pump, Topology, Task, Supervisor, Monitor, Metastore |
| `kafka` | Kafka-specific Source (consumer group) and Sink (producer) using Shopify/sarama, plus encoders |
| `cache` | Cache-backed sink for write-through caching |
| `channel` | Go channel-based Source and Sink for in-process/test usage |
| `sql` | SQL database sink using `database/sql` |
| `mocks` | Custom test mocks for Pipe, Source, Predicate |
| `example` | Example applications demonstrating library usage |

## Key Interfaces
- **Source** -- `Consume() (Message, error)`, `Commit(interface{}) error`, `Close() error`
- **Processor** -- `WithPipe(Pipe)`, `Process(Message) error`, `Close() error`
- **Committer** -- extends Processor with `Commit(ctx) error` for batch operations
- **Pipe** -- `Mark(Message)`, `Forward(Message)`, `ForwardToChild(Message, int)`, `Commit(Message)`
- **Pump** -- async/sync message pump with `Accept(Message)`, `Stop()`, `Close()`
- **Supervisor** -- manages global commit lifecycle, ensures concurrency safety
- **Monitor** -- collects processing events (latency, throughput, back-pressure)
- **Node** -- topology graph node (`SourceNode`, `ProcessorNode`)
- **Mapper / FlatMapper / Predicate** -- functional transformation and filtering interfaces

## Data Flow
1. `StreamBuilder` constructs a `Topology` (DAG of nodes)
2. `Task.Start()` wires up the topology: creates Pipes, Pumps (sync or async), SourcePumps
3. `SourcePump` goroutine calls `Source.Consume()` in a loop, dispatching messages to child Pumps
4. Each `Pump` invokes its `Processor.Process()`, which may `Forward()` messages to children via the `Pipe`
5. `Metastore` tracks metadata (offsets) per Source across processors
6. `Supervisor` coordinates global commits -- merges metadata and calls `Source.Commit()`
7. `TimedSupervisor` wraps Supervisor to trigger periodic auto-commits
8. `Monitor` aggregates latency/throughput events and flushes to `Stats`

## External Dependencies
- **Apache Kafka** -- primary external system via `kafka` package (consumer groups, producers)
- **SQL databases** -- via `sql` package (any `database/sql` compatible driver)
- **Cache systems** -- via `cache` package (abstracted behind interface)
