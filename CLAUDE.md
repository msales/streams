@~/CLAUDE.md

## Project Description

**Language:** Go 1.17  
**Module:** `github.com/msales/streams/v6`  
**Purpose:** A lightweight stream processing library. While Kafka is the primary use case, it supports processing from any source via a pluggable Source/Processor/Pipe architecture.  
**Key dependencies:** Shopify/sarama (Kafka client), msales/pkg/v4 (internal utilities), stretchr/testify (testing), DATA-DOG/go-sqlmock (SQL test mocks), xdg/scram (SASL/SCRAM auth), golang.org/x/xerrors (error wrapping).

## Directory Structure

```
cache/          Cache-based sink (write-through cache processor)
channel/        Go channel-based source and sink (useful for testing/in-process)
example/        Example applications (simple, branch, merge, kafka, benchmark)
kafka/          Kafka source, sink, and encoder implementations using Shopify/sarama
mocks/          Mock implementations for Pipe, Source, Predicate (custom mock framework)
sql/            SQL-based sink for database writes
```

## Build, Test & Lint

```bash
# Build
go build ./...

# Run tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Vet
go vet ./...
```

The Makefile includes shared targets from `github.com/msales/make/golang`. The Dockerfile is for CI testing only (library, not a standalone service).

## Code Style

### Imports

Three groups separated by blank lines:
1. Standard library
2. Third-party packages
3. Internal packages (`github.com/msales/streams/v6/...`)

### Naming Conventions

- **Packages:** lowercase, single-word (e.g., `kafka`, `cache`, `channel`, `mocks`)
- **Interfaces:** noun-based, describe capability (e.g., `Processor`, `Source`, `Pipe`, `Pump`, `Monitor`, `Supervisor`, `Committer`)
- **Exported types:** PascalCase with descriptive names; unexported types use camelCase
- **Functional options:** `TaskOptFunc` pattern with `With*` constructor functions (e.g., `WithCommitInterval`, `WithMode`)

### Error Handling

- Errors returned as `error` values, propagated up the call chain
- Sentinel errors defined as package-level vars (e.g., `ErrNotRunning`, `ErrAlreadyRunning`)
- `ErrorFunc` callback pattern for async error handling in goroutines
- `golang.org/x/xerrors` used for error wrapping in some places

### Testing

- **Framework:** `stretchr/testify` (assert package, no suite usage)
- **Pattern:** one test function per behavior, named `Test<Type>_<Method>` and `Test<Type>_<Method>With<Condition>`
- **Mocks:** custom mock framework in `mocks/` package (not testify/mock) with expectation-based assertions
- **Internal tests:** `_internal_test.go` files for white-box testing of unexported functions
- **External tests:** `_test.go` files in `_test` package for black-box testing

## Branching & Git

- **Never** work directly on `main` or `master`. Always create a new branch off the latest `main`/`master`.
- Before creating a branch, **always pull** the latest changes from the remote (`git pull origin master`).
- If the user provides a task identifier in the format `TRK-XXXX`, use it as the branch name (e.g., `TRK-1234`). Otherwise, **ask the user** for a task ID or branch name before proceeding.
- **Never push** to `main` or `master` directly.
- Commit changes after each meaningful iteration. Use your judgment to decide when progress should be saved -- prefer smaller, atomic commits over large monolithic ones.
- Write clear, concise commit messages. Use conventional commit format when appropriate (e.g., `feat:`, `fix:`, `refactor:`, `test:`).
