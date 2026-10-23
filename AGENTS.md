# Repository guidance

## Project

Metric Ferry (`metric-ferry`) is a Go command-line tool that streams
zstd-compressed, timestamped OpenMetrics/Prometheus text histories into Prometheus remote write, OTLP
HTTP/protobuf, or both. It supports timestamp shifting/rebasing and OTLP attribute
mapping. Read `README.md` for the quick start and `docs/usage.md` for the input
contract, CLI options, and delivery limits.

The repository is a single `package main` module. Go 1.26 or newer is required,
as specified in `go.mod`. Decompression happens inside the process; the tool does
not require an external zstd executable, an OpenTelemetry Collector, or a metrics
SDK.

## Code map

- `main.go`: CLI entry point, signal handling, and top-level error reporting.
- `config.go`: flags, defaults, configuration validation, and HTTP headers.
- `input.go`: compressed input, timestamp parsing/arithmetic, and end-anchor scans.
- `parse.go`: text parsing, label identity, series caching, and metric metadata.
- `mapping.go`: OTLP semantic attribute names, resource placement, and custom rules.
- `encode.go`: Prometheus/OTLP protobuf construction and request compression.
- `pipeline.go`: sample validation, deduplication, worker sharding, and progress.
- `transport.go`: HTTP delivery, retries, partial responses, and batch splitting.
- `*_test.go`: unit tests, local HTTP receiver integration tests, benchmarks,
  and parser fuzzing.
- `mapping.example.json`: example custom attribute mapping.
- `examples/`: a tiny synthetic history and its reproducible compressed fixture.
- `.github/`: CI, dependency updates, and contribution templates.

## Development and validation

Build with `go build -o metric-ferry .`. For Go changes, format modified files with
`gofmt` and run:

```sh
go test -race ./...
go vet ./...
```

Use focused tests while iterating. Add regression coverage for behavior changes,
especially parsing, timestamp precision, ordering, mapping, and delivery errors.
The integration tests use local HTTP receivers; they do not require a running
Prometheus or Collector instance. Documentation-only changes do not require Go
tests.

When relevant to parser or performance changes, use:

```sh
go test -run '^$' -bench . -benchmem
go test -run '^$' -fuzz FuzzSampleParsing -fuzztime 10s
```

Run `./metric-ferry -input examples/demo.om.zst -dry-run` for a bundled smoke test.
With a local input archive, `./metric-ferry -input path/to/metrics.om.zst -dry-run`
exercises parsing, mapping, serialization, and compression without sending data.
Without destination flags, a dry run encodes both protocols. A run with `-limit`
does not validate the remainder of the archive or its final checksum.

## Behavior to preserve

- Stream input with bounded buffers and explicit series limits. Do not load the
  full decompressed archive into memory or temporary files. Preserve the capped
  label cache and avoid per-sample allocations in the cached parsing path.
- Maintain timestamp order within each series through worker assignment,
  batching, retries, and oversized-request splitting. Global series ordering is
  not required. Reject conflicting samples and mappings that merge source series.
- Preserve integer timestamp precision and checked arithmetic: input defaults to
  decimal seconds, internal timestamps and OTLP use nanoseconds, and remote write
  uses milliseconds. End-anchor rebasing scans for the maximum timestamp before
  reopening the file; first-anchor rebasing uses the first encountered sample.
- Keep Prometheus metric names and label names unchanged. Default OTLP mapping
  restores known semantic keys while preserving unknown application labels.
  Explicit mappings and resource/data-point placement rules must retain their
  documented precedence.
- Histograms and summaries remain separate scalar series. Exemplars require
  explicit permission through `-drop-exemplars` to discard. Do not silently
  broaden or weaken the documented input contract.
- Keep dry runs on the normal validation and encoding paths. They must not send
  HTTP requests.
- Preserve protocol-specific compression and retry rules, cancellation, and
  sequential delivery per series. Do not retry OTLP partial rejections or imply
  transactional delivery, rollback, or exactly-once guarantees.

## Change conventions

Follow the existing small-function, standard Go style and keep changes focused.
Update `README.md` and `docs/usage.md` when flags, defaults, mapping behavior, input
support, or delivery guarantees change. Keep generated binaries, compressed
datasets, profiles, and coverage output out of version control; see `.gitignore`.
The tiny `examples/demo.om.zst` fixture is an intentional exception: keep it in
sync with `examples/demo.om` using the command in `examples/README.md`.
