# Benchmarking Metric Ferry

[Back to Metric Ferry](../README.md)

Measure local parsing/encoding separately from receiver ingestion. A dry run
does not measure network throughput, backend limits, or storage performance.
The tiny bundled demo is for correctness checks, not throughput comparisons.

## Cached parser benchmark

```sh
go test -run '^$' -bench BenchmarkCachedParse -benchmem -count 5
```

This measures the cached parser path, not decompression, protobuf encoding,
compression, or HTTP delivery. Report `ns/op`, `B/op`, and `allocs/op` together.

## End-to-end local processing

Build once, then run against a representative archive:

```sh
go build -o metric-ferry .
go version -m ./metric-ferry

# GNU time on Linux reports wall time and peak resident memory.
/usr/bin/time -v ./metric-ferry -input metrics.om.zst -dry-run -progress 0
```

Without destination flags, a dry run encodes both protocols. To measure just one,
specify its destination flag together with `-dry-run`; no request is sent.

For a useful report, include:

- Git commit, Go version, OS, CPU, and available RAM.
- Exact command, worker count, compression, and any changed limits.
- Compressed and decompressed sizes, sample count, and distinct series count.
- Wall time, peak RSS, and repeated-run results; note whether the file cache was warm.
- Whether rebasing was enabled. End-anchor rebasing reads the archive twice, and
  the progress timer starts after the initial timestamp scan, so compare total
  wall time when including that scan.

Do not infer scalability from the cached parser benchmark alone. Receiver tests
also need the receiver version/configuration, network conditions, retry counts,
and accepted point counts. Use a dedicated test receiver: delivery is not
transactional and there is no automatic rollback.

The [previously recorded measurement](usage.md#throughput-and-delivery) is
provided with its hardware and caveats. Its input archive is not bundled.
