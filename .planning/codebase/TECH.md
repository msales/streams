# Technology Stack

## Language & Runtime
Go 1.17

## Dependencies
| Dependency | Purpose |
|---|---|
| `github.com/Shopify/sarama` | Apache Kafka client library (consumer groups, producers) |
| `github.com/msales/pkg/v4` | Internal shared utilities (syncx.Mutex for try-lock) |
| `github.com/stretchr/testify` | Test assertions |
| `github.com/DATA-DOG/go-sqlmock` | SQL driver mock for testing SQL sink |
| `github.com/xdg/scram` | SASL/SCRAM authentication for Kafka |
| `golang.org/x/xerrors` | Extended error wrapping |

## Build System
- **Makefile:** Includes shared targets from `github.com/msales/make/golang`
- **Dockerfile:** CI-only image based on `msales/go-builder:1.17-base-1.0.0`; runs `go build -v ./...` to verify compilation
- No binary output (this is a library)

## Testing
- `stretchr/testify/assert` for assertions
- Custom mock framework in `mocks/` package with expectation-based pipe and source mocks
- `go-sqlmock` for SQL sink tests
- Both internal (`_internal_test.go`) and external (`_test.go`) test files

## CI/CD
- GitHub Actions workflow referenced in README badges (`test.yaml`)
- Coveralls integration for coverage reporting
