# A small history you can try

[Back to Metric Ferry](../README.md)

`demo.om` is a synthetic checkout-service history: 12 samples, 3 series, and
45 seconds of data starting at `2026-01-01T00:00:00Z`. It includes a counter,
two gauges, explicit timestamps, and labels that demonstrate OTLP resource and
data-point mapping. It contains no production data.

`demo.om.zst` is the same history compressed with zstd. It is deliberately checked
in so trying Metric Ferry does not require a separate compressor.

Run these commands from the repository root:

```sh
go build -o metric-ferry .

# Validate and encode both protocols without a receiver.
./metric-ferry -input examples/demo.om.zst -dry-run -progress 0

# Exercise end-anchor rebasing without sending data.
./metric-ferry -input examples/demo.om.zst -dry-run \
  -rebase 2026-10-20T12:00:00Z -progress 0

# Exercise stdin with a fixed timestamp offset.
cat examples/demo.om.zst | ./metric-ferry -input - -dry-run -shift 24h -progress 0
```

Each command reports `samples=12`, `series=3`, `prometheus=12`, and `otlp=12`.
Input is ordered within each series; global timestamp ordering is unnecessary.
`service_name` and `service_namespace` become OTLP resource attributes;
`http_response_status_code` becomes a data-point attribute.

To try an actual receiver, follow the [quick start](../README.md#send-an-archive).
Use `-rebase now` to put the newest sample at the present time. Imports are sent
as quickly as the receiver accepts them, rather than waiting 15 seconds between
samples.

## Regenerate the compressed example

Only maintainers changing the fixture need the `zstd` CLI:

```sh
zstd -q -f -3 examples/demo.om -o examples/demo.om.zst
zstd -q -d -c examples/demo.om.zst | cmp - examples/demo.om
```

Keep this example tiny and synthetic. Use temporary files or ignored archives
for large experiments.
