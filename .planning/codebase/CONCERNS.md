# Concerns & Risks

## Technical Debt
- Go 1.17 is significantly outdated (current stable is 1.22+); no generics usage
- `golang.org/x/xerrors` is deprecated in favor of standard `errors` package (Go 1.13+)
- `github.com/Shopify/sarama` has been archived/moved to `github.com/IBM/sarama`
- Makefile relies on external shared make includes (`github.com/msales/make/golang`) -- opaque build targets
- Hard-coded channel buffer sizes (1000) in async pump and monitor with no configurability
- `nanotime.s` uses assembly for high-resolution timing -- non-portable, ties to specific architectures

## Missing Coverage
- No benchmarks in test suite despite `example/benchmark` directory existing
- No integration tests for Kafka (only unit tests with mocked interfaces)
- `cache/` package sink tests may be minimal (only one test file)
- No fuzz testing

## Security
- Dockerfile passes `GITHUB_TOKEN` as build arg -- visible in image layers if not using multi-stage build properly
- SASL/SCRAM support is present for Kafka authentication, which is good
- No TLS configuration examples or documentation

## Performance
- Async pump uses fixed 1000-element buffered channel -- may cause back-pressure issues or memory waste depending on workload
- Monitor event channel also fixed at 1000 -- high-throughput scenarios could cause event drops or blocking
- `flattenNodeTree` uses O(n^2) algorithm for dependency ordering in topology
- `nanotime` assembly optimization suggests performance is a design priority
- No configurable concurrency for source pumps (one goroutine per source)
