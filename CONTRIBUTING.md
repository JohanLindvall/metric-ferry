# Contributing to Metric Ferry

Small bug fixes, documentation improvements, receiver compatibility reports,
and reproducible benchmarks are welcome. For a larger change, open an issue
describing the problem and intended behavior before investing in an implementation.

## Local setup

Use Go 1.26 or newer. No external Prometheus, Collector, or zstd executable is
needed for the tests or bundled demo.

```sh
git clone https://github.com/JohanLindvall/metric-ferry.git
cd metric-ferry
go build -o metric-ferry .
./metric-ferry -input examples/demo.om.zst -dry-run -progress 0
go test -race ./...
go vet ./...
```

Format modified Go files with `gofmt`. CI checks formatting, module integrity,
tests with the race detector, vet, the build, and the bundled demo on the minimum
supported Go release and the current stable release.

## Preparing a change

- Use a branch and open a pull request with the problem, resulting behavior,
  and commands you ran to validate it.
- Add a focused regression test when changing behavior. Existing HTTP receiver
  tests decode actual protobuf requests and check ordering and error handling.
- Keep memory bounded by configured limits, preserve per-series order through
  retries, and retain timestamp precision. See [AGENTS.md](AGENTS.md) for the code
  map and implementation constraints.
- Update the [usage reference](docs/usage.md) for changes to flags, defaults,
  mappings, supported input, or delivery guarantees.
- For performance changes, include before/after measurements with the same
  input, command, and hardware. See [benchmarking](docs/benchmarks.md).

Documentation-only changes do not require Go tests. Check relative links and
run any new example commands where applicable.

## Reporting a problem

Use the [issue forms](https://github.com/JohanLindvall/metric-ferry/issues/new/choose).
Include the commit or version (`go version -m ./metric-ferry`), operating system,
receiver and version, sanitized command, and a minimal sample when possible.
Remove authorization headers, tokens, private endpoints, and sensitive labels
from commands and metric data before posting them.

Report vulnerabilities through [SECURITY.md](SECURITY.md). Our
[code of conduct](CODE_OF_CONDUCT.md) applies to project interactions.

Metric Ferry is [MIT licensed](LICENSE). Contributions are made under the same
license; retain notices required by any code you reuse.
